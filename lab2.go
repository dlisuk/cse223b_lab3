package triblab

import (
	"trib"
	"sort"
	"time"
	"trib/colon"
	"errors"
	"bytes"
	"strconv"
	"strings"
)

//See keeper.go for ServeKeeper function
//See bin_storage.go for NewBinClient function
//Here we put the front end logic
const (
	usersBin  = "_USERS_"
	tribsKey  = "_TRIBS_"
	flagTrue  = "True"
)

func NewFront(s trib.BinStorage) trib.Server {
	return &front{ s }
}

type front struct{
	bin         trib.BinStorage
}

func (self *front) isUser(user string) (bool, error) {
	userList := self.bin.Bin(usersBin)
	var exists string
	err := userList.Get(user, &exists)
	if err != nil { return false, err }
	return exists == flagTrue, nil
}
// Creates a user
func (self *front) SignUp(user string) error {
	//Check if username is valid
	if len(user) > trib.MaxUsernameLen { return errors.New("Username '" + user + "' Exceeds Max Length " + strconv.FormatInt(trib.MaxUsernameLen,10))}
	if !trib.IsValidUsername(user)     { return errors.New("Username '" + user + "' Is Invalid") }

	//Check if user already exists
	exists, err := self.isUser(user)
	if err != nil { return err }
	if exists     { return errors.New("User '" + user + "' Already Exists") }

	//Create a key user -> flagTrue in the usersBin
	var succ bool
	userList := self.bin.Bin(usersBin)
	err = userList.Set(trib.KV(user, flagTrue), &succ)
	if err != nil { return err }
	if !succ      { return errors.New("Failure Adding User '" + user +"'") }
	return nil
}

// List 20 registered users.  When there are less than 20 users that
// signed up the service, all of them needs to be listed.  When there
// are more than 20 users that signed up the service, an arbitrary set
// of at lest 20 of them needs to be listed.
// The result should be sorted in alphabetical order
func (self *front) ListUsers() ([]string, error){
	var users trib.List
	userList := self.bin.Bin(usersBin)
	err := userList.Keys(&trib.Pattern{"",""}, &users)

	if err != nil{ return nil, err }
	if len(users.L) > 20{

	}
	sort.Strings(users.L)
	return users.L, nil
}

// Post a tribble.  The clock is the maximum clock value this user has
// seen so far by reading tribbles or clock sync.
func (self *front) Post(who, post string, clock uint64) error{
	//First we have to ensure the poster is a real user
	exists, err := self.isUser(who)
	if err != nil { return err }
	if !exists    { return errors.New("User '" + who + "' does not exist and thus cannot post-" + post) }

	//Now we make the tribble and convert to a string
	t          := trib.Trib{who, post, time.Now(), clock}
	tribString := self.tribToString(t)

	//Get the users's bin and add the tribble string to their list of tribbles
	userStore := self.bin.Bin(who)
	var succ bool
	err = userStore.ListAppend(trib.KV(tribsKey, tribString), &succ)
	if err != nil { return err }
	if !succ      { return errors.New("Unspecified error submitting tribble-" + tribString) }
	return nil
}

// List the tribs that a particular user posted
func (self *front) Tribs(user string) ([]*trib.Trib, error){
	//First we have to ensure the poster is a real user
	exists, err := self.isUser(user)
	if err != nil { return nil, err }
	if !exists    { return nil, errors.New("User '" + user + "' does not exist and thus has no tribbles") }

	//Get the users's bin and get their list of tribbles
	userStore := self.bin.Bin(user)
	var rawList trib.List
	err = userStore.ListGet(tribsKey, &rawList)
	if err != nil {return nil, err}

	outList := make([]*trib.Trib,0,len(rawList.L))
	for i := range rawList.L{
		trib, err := self.stringToTrib(rawList.L[i])
		if err != nil { return nil, err }
		outList = append(outList, trib)
	}

	//Sort in tribble order and then grab the maximum number of tribbles to return
	sort.Sort(tribOrder(outList))
	if len(outList) > trib.MaxTribFetch { outList = outList[(len(outList)-trib.MaxTribFetch):len(outList)] }
	return outList, nil
}

// Follow someone's timeline
func (self *front) Follow(who, whom string) error{
	if who == whom {return errors.New("User cannot follow themselves")}
	exists, err := self.isUser(who)
	if err != nil {return err}
	if !exists     { return errors.New("Following user '" + who + "' does not exist") }
	exists, err = self.isUser(whom)
	if err != nil {return err}
	if !exists     { return errors.New("Followed user '" + whom + "' does not exist") }
	following, err := self.IsFollowing(who,whom)
	if err != nil {return err}
	if following {return errors.New(who + " is already following " + whom)}

	userStore := self.bin.Bin(who)
	var succ bool
	err = userStore.Set(trib.KV(whom, flagTrue), &succ)
	if err != nil { return err }
	if !succ      { return errors.New("Unspecified error when '" + who +"' follows '" + whom +"'") }
	return nil
}

// Unfollow
func (self *front) Unfollow(who, whom string) error{
	if who == whom { return errors.New("User cannot unfollow themselves") }
	exists, err := self.isUser(who)
	if err != nil  { return err }
	if !exists     { return errors.New("Following user '" + who + "' does not exist") }
	exists, err = self.isUser(whom)
	if err != nil  { return err }
	if !exists     { return errors.New("Followed user '" + whom + "' does not exist") }
	following, err := self.IsFollowing(who,whom)
	if err != nil  { return err }
	if !following  { return errors.New(who + " is not following " + whom) }

	userStore := self.bin.Bin(who)
	var succ bool
	err = userStore.Set(trib.KV(whom, ""), &succ)
	if err != nil { return err }
	if !succ      { return errors.New("Unspecified error when '" + who +"' unfollows '" + whom +"'") }
	return nil
}

// Returns true when who following whom
func (self *front) IsFollowing(who, whom string) (bool, error){
	if who == whom { return false, errors.New("User cannot follow themselves")}
	exists, err := self.isUser(who)
	if err != nil  { return false, err}
	if !exists     { return false, errors.New("Following user '" + who + "' does not exist") }
	exists, err =  self.isUser(whom)
	if err != nil  { return false, err}
	if !exists     { return false, errors.New("Followed user '" + whom + "' does not exist") }

	userStore := self.bin.Bin(who)
	var following string
	err = userStore.Get( whom, &following)
	if err != nil { return false, err }
	return following == flagTrue, nil
}

// Returns the list of following users
func (self *front) Following(who string) ([]string, error){
	exists, err := self.isUser(who)
	if err != nil  { return nil, err}
	if !exists     { return nil, errors.New("User '" + who + "' does not exist") }

	userStore := self.bin.Bin(who)
	var followList trib.List
	err = userStore.Keys(&trib.Pattern{"",""},&followList)
	if err != nil { return nil, err }
	return followList.L, nil
}

// List the trib of someone's following users
func (self *front) Home(user string) ([]*trib.Trib, error){
	exists, err := self.isUser(user)
	if err != nil  { return nil, err}
	if !exists     { return nil, errors.New("User '" + user + "' does not exist") }
	followees, err := self.Following(user)
	outList := make([]*trib.Trib,0,trib.MaxFollowing*trib.MaxTribFetch)
	for i := range followees{
		tribs,err := self.Tribs(followees[i])
		if err != nil  { return nil, err}
		for j := range tribs{
			outList = append(outList, tribs[j])
		}
	}
	sort.Sort(tribOrder(outList))
	if len(outList) > trib.MaxTribFetch { outList = outList[(len(outList)-trib.MaxTribFetch):len(outList)] }
	return outList, nil
}

func (self *front) tribToString(t trib.Trib) string{
	var tribString bytes.Buffer
	tribString.WriteString(colon.Escape(strconv.FormatUint(t.Clock,36)))
	tribString.WriteString("::")
	tribString.WriteString(colon.Escape(strconv.FormatInt(t.Time.Unix(),36)))
	tribString.WriteString("::")
	tribString.WriteString(colon.Escape(t.Message))
	tribString.WriteString("::")
	tribString.WriteString(colon.Escape(t.User))
	return tribString.String()
}

func (self *front) stringToTrib(tribString string) (*trib.Trib, error) {
	tribFields := strings.Split(tribString, "::")
	if len(tribFields) != 4 { return nil, errors.New("Insufficient Fields In Tribble: " + tribString) }

	clock, err   := strconv.ParseUint(tribFields[0], 36, 64)
	if err != nil { return nil, err }

	timeInt, err := strconv.ParseInt(colon.Unescape(tribFields[1]), 36, 64)
	if err != nil { return nil, err}
	time         := time.Unix(timeInt,0)

	msg          := colon.Unescape(tribFields[2])

	user         := colon.Unescape(tribFields[3])

	return &trib.Trib{user, msg, time, clock}, nil
}

type tribOrder []*trib.Trib
func (v tribOrder) Len() int { return len(v) }
func (v tribOrder) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v tribOrder) Less(i, j int) bool {
	if v[i].Clock != v[j].Clock { return v[i].Clock < v[j].Clock }
	if !v[i].Time.Equal(v[j].Time) { return v[i].Time.Before( v[j].Time ) }
	if v[i].User != v[j].User { return v[i].User < v[j].User }
	return v[i].Message < v[j].Message
}
