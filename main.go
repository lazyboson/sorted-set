package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

type Data struct {
	status string `json:"status"`
	val    string `json:"val"`
}

func main() {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		// handle error
	}
	defer conn.Close()

	key := "my_sorted_set"
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
	err = addToSS(key, "rahoul", "4")
	if err != nil {
		fmt.Println(err)
	}

	_ = &Data{
		status: "completed",
		val:    "5",
	}

	err = AddKeyVal("ashutosh", "ashu")
	if err != nil {
		fmt.Println(err)
	}
	err = AddKeyVal("pooja", "ashu")
	if err != nil {
		fmt.Println(err)
	}
	err = AddKeyVal("yogita", "ashu")
	if err != nil {
		fmt.Println(err)
	}
	err = AddKeyVal("priya", "ashu")
	if err != nil {
		fmt.Println(err)
	}
	err = AddKeyVal("rahul", "ashyu")
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
    	    if value == "ashu" then
    	        table.insert(result, { member, score })
    	        if #result == tonumber(KEYS[2]) then
    	            break
    		        end
		        end
	    	end
		end

	return result
	`)

	result, err := redis.Values(script.Do(conn, key, 2))
	if err != nil {
		// handle error
		fmt.Println(err)
		return
	}
	membersWithScore := make(map[string]uint32)
	for i := 0; i < len(result); i++ {
		memberSco, _ := redis.Values(result[i], nil)
		member := string(memberSco[0].([]byte))
		sScore := string(memberSco[1].([]byte))
		u32, _ := strconv.ParseUint(sScore, 10, 32)
		membersWithScore[member] = uint32(u32)
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

func AddKeyVal(key string, data string) error {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		// handle error
	}
	defer conn.Close()

	//dataBytes, _ := json.Marshal(data)

	_, err = redis.String(conn.Do("SET", key, data))
	if err != nil {
		return err
	}

	return nil
}
