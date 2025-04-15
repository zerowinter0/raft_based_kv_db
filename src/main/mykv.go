package main

import (
	"fmt"
	"strconv"

	"6.5840/kvraft"
)

// Check that ops are committed fast enough, better than 1 per heartbeat interval
func main() {
	fmt.Println("请输入节点数:")
	fmt.Print(">>> ")
	serverNum := 10
	fmt.Scanln(&serverNum)
	cfg := kvraft.MakeRealConfig(serverNum, false, -1)
	defer cfg.Cleanup()

	ck := cfg.MakeClient(cfg.All())

	fmt.Println("输入'exit'退出,输入'get X'查询,输入'put X Y'插入,输入'append X Y'追加, 输入'kill X'杀死节点，输入'start X'启动节点:")

	live_set := make(map[int]int, 0)
	for i := 0; i < serverNum; i++ {
		live_set[i] = 1
	}
	for {
		fmt.Print(">>> ")
		var op string
		var key string
		var value string
		length, _ := fmt.Scanln(&op, &key, &value)
		if length == 1 && op == "exit" {
			break
		} else if length == 2 && op == "get" {
			val := ck.Get(key)
			fmt.Println("值:" + val)
		} else if length == 3 && op == "put" {
			ck.Put(key, value)
		} else if length == 3 && op == "append" {
			ck.Append(key, value)
		} else if length == 2 && op == "kill" {
			num, err := strconv.Atoi(key)
			if err != nil {
				fmt.Println("转换错误:", err)
			} else if _, ok := live_set[num]; !ok {
				fmt.Printf("%d节点已死亡\n", num)
			} else if len(live_set) == serverNum/2+1 {
				fmt.Println("活跃节点数已达最低值，不能再杀死节点")
			} else {
				cfg.ShutdownServer(num)
				delete(live_set, num)
			}
		} else if length == 2 && op == "start" {
			num, err := strconv.Atoi(key)
			if err != nil {
				fmt.Println("转换错误:", err)
			} else if _, ok := live_set[num]; ok {
				fmt.Printf("%d节点已启动\n", num)
			} else {
				cfg.StartServer(num)
				live_list := make([]int, 0)
				for key, _ := range live_set {
					live_list = append(live_list, key)
				}
				cfg.Connect(num, live_list)
				live_set[num] = 1
			}
		}
	}

	cfg.End()
}
