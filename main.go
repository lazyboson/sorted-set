package main

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

type Data struct {
	Status string `json:"status"`
	Val    string `json:"val"`
}

func main() {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		// handle error
	}
	defer conn.Close()

	key := "myset"
	err = addToSS(key, "ashutosh", "0")
	if err != nil {
		fmt.Println(err)
	}
	err = addToSS(key, "pooja", "1")
	if err != nil {
		fmt.Println(err)
	}
	err = addToSS(key, "yogita", "2")
	if err != nil {
		fmt.Println(err)
	}
	err = addToSS(key, "priya", "3")
	if err != nil {
		fmt.Println(err)
	}
	err = addToSS(key, "rahul", "4")
	if err != nil {
		fmt.Println(err)
	}

	data := &Data{
		Status: "completed",
		Val:    "5"}
	err = AddKeyVal("ashutosh", data)
	if err != nil {
		fmt.Println(err)
	}
	err = AddKeyVal("pooja", data)
	if err != nil {
		fmt.Println(err)
	}
	err = AddKeyVal("yogita", data)
	if err != nil {
		fmt.Println(err)
	}
	err = AddKeyVal("priya", data)
	if err != nil {
		fmt.Println(err)
	}
	data = &Data{
		Status: "complet",
		Val:    "5"}
	err = AddKeyVal("rahul", data)
	if err != nil {
		fmt.Println(err)
	}

	var script = redis.NewScript(2, `
		local result = {}
		local members_with_scores = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
		for i = 1, #members_with_scores, 2 do
    	local member = members_with_scores[i]
    	local score = members_with_scores[i + 1]
    	local value = redis.call('GET', member)
    	if value then
			local data = cjson.decode(value)
    	    if data.status == "completed" or data.val == "5" then
    	        table.insert(result, { member, score })
    	        if #result == tonumber(KEYS[2]) then
    	            break
    		        end
		        end
	    	end
		end

	return result
	`)

	result, err := redis.Values(script.Do(conn, key, 8))
	if err != nil {
		// handle error
		fmt.Println(err)
		return
	}
	membersWithScore := make(map[string]uint32)
	for i := 0; i < len(result); i++ {
		memberSco, err := redis.Values(result[i], nil)
		if err != nil {
		}
		if len(memberSco) == 2 {
			member, okMember := memberSco[0].([]byte)
			sScore, oksScore := memberSco[1].([]byte)
			if !okMember || !oksScore {
			}
			score, _ := strconv.ParseUint(string(sScore), 10, 32)
			membersWithScore[string(member)] = uint32(score)
		}
	}

	fmt.Println(membersWithScore)
}

func addToSS(key, member, score string) error {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		// handle error
	}
	defer conn.Close()

	_, err = redis.Int(conn.Do("ZADD", key, score, member))
	if err != nil {
		return err
	}

	return nil
}

func AddKeyVal(key string, data *Data) error {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		// handle error
	}
	defer conn.Close()

	dataBytes, _ := json.Marshal(data)

	_, err = redis.String(conn.Do("SET", key, string(dataBytes)))
	if err != nil {
		return err
	}

	return nil
}
