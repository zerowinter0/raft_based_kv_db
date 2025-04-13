package raft

// //
// // Raft tests.
// //
// // we will use the original test_test.go to test your code for grading.
// // so, while you can modify this code to help you debug, please
// // test with the original before submitting.
// //

// import (
// 	"fmt"
// 	"math/rand"
// 	"testing"
// )

// func GenerateMyRandomString(length int) string {
// 	bytes := make([]byte, length)
// 	for i := range bytes {
// 		bytes[i] = byte(rand.Intn(256)) // 生成 0-255 的随机 byte
// 	}
// 	return string(bytes)
// }

// func MyFuzz() {
// 	// 每个测试用例独立初始化系统状态

// 	inputs_str := GenerateMyRandomString(1000)

// 	input_set := make([]string, 0)
// 	var t *testing.T
// 	for {
// 		servers := 10
// 		max_disconnect_num := 4
// 		idx := 0

// 		//inputs_str := string("asod\xe7\xe7\xe7\xe7fguhaosdfghaoisdufhaoUdhfk\x94\x7f\xfcN\xced\xf8\xceR\xf4\x9f\x1b\u05ca\vi\xb5R\x85j\x9d\x82\xe3\x85M7)\xcbu\x06\x1d\x82Q$\x02\xe3>\r\xfdG #\xe3\xeb|n\xe2\xe5\xc0c:\xc5܇*R\xafg\xcb)3\x8b\x16#~\x14IizP\x18\xd3\xe4\\\xca\xe6Nx\xfe\xd5\xcb\xc3\x1a\xfd\xbdMzV\xb5*wןk\xbd\xde\xc1\x93\xbe\xd7\xe4\x9asW\x9f\x8eg\xca4am>\xaa4\x13@\xba\xa40\x98\xb1hh\x8e\x97e\x14\xfc\xa96ڃ\x14'\x95\xab\b\xcc\x0f\xb1\xa0mh\xb5\xb3\x9b\xab\x7f\x87X\x83\xdd\x7fp\xf6[Uŗp\x8bƀ\x8d\xf6\x8cg)(\x18\xa8<\xa2I\x8f\x979\x80\rɿ\x95%\xfc\u05ec\xa1\xde\xe4\xd0\xd1\xd2\x14\xb9u\xb9\xbdAࣲ\x8b3\x15s\xfd\xac%\xae\xb8/,c\xa2|\xe1g\xa5\xd1=\xee+y\x80%\xc20\x89\x8c\xe0j\x95 \xe0\x82!؟\xf2\xe8\xc6\x16\xa9\x9e\xc9Ρu\x9f\x03\x8e\"\x06-\x01K|6x;\xec\xb0#\xea\xd3\x156\x8e\x8eaTc\xcc1\x9d/\xb6\xe8\xd3&d\xc5Up\xfc\xa14{6\xf5\xd1\xffG\xfc\x10W)>\x11ǡ\xa2o\xad]\xb4\\f\x96\x80\xecvY\xf5\xd5\xfb\x1f(#K\xa6asjdhfkalsjdhfklashdnfhaosdhffl")
// 		inputs := make([]int64, len(inputs_str))
// 		for i := 0; i < len(inputs_str); i++ {
// 			inputs[i] = int64(inputs_str[i])
// 		}
// 		cfg_input := make([]int64, servers+1)
// 		for i := 0; i < servers+1; i++ {
// 			cfg_input[i] = inputs[idx]
// 			idx++
// 		}

// 		cfg := make_config(t, servers, false, false, cfg_input...)
// 		defer func() {
// 			if r := recover(); r != nil {
// 				t.Errorf("Panic occurred: %v", r)
// 			}
// 			// 确保清理逻辑在panic后执行
// 			cfg.cleanup() // 显式调用清理，避免defer未执行
// 		}()
// 		cfg.begin("Fuzzing start!")

// 		disconnect_cnt := 0
// 		disconnect_set := make(map[int]int, 0)
// 		killed_set := make(map[int]int, 0)
// 		for ; idx < len(inputs); idx++ {
// 			input := int(inputs[idx])
// 			fmt.Printf("current input:%d\n", input)
// 			flag := input % 10
// 			target_server := (input / 10) % 10
// 			if _, exists := disconnect_set[target_server]; flag == 2 && disconnect_cnt < max_disconnect_num && !exists {
// 				if _, dead := killed_set[target_server]; !dead {
// 					cfg.disconnect(target_server)
// 					disconnect_cnt++
// 					disconnect_set[target_server] = 1
// 					fmt.Printf("%d disconnect\n", target_server)
// 				}
// 			} else if _, exists := disconnect_set[target_server]; flag == 3 && exists {
// 				if _, dead := killed_set[target_server]; !dead {
// 					cfg.connect(target_server)
// 					disconnect_cnt--
// 					delete(disconnect_set, target_server)
// 					fmt.Printf("%d reconnect\n", target_server)
// 				}
// 			} else if _, exists := killed_set[target_server]; flag == 4 && disconnect_cnt < max_disconnect_num && !exists {
// 				if _, disconnect := disconnect_set[target_server]; disconnect {
// 					delete(disconnect_set, target_server)
// 				} else {
// 					cfg.disconnect(target_server)
// 					disconnect_cnt++
// 				}

// 				cfg.crash1(target_server)

// 				killed_set[target_server] = 1
// 				fmt.Printf("%d killed\n", target_server)
// 			} else if _, exists := killed_set[target_server]; flag == 5 && exists {
// 				cfg.start1(target_server, cfg.applier, inputs[idx])
// 				disconnect_cnt--
// 				delete(killed_set, target_server)
// 				cfg.connect(target_server)
// 				delete(disconnect_set, target_server)
// 				fmt.Printf("%d recover\n", target_server)
// 			} else {
// 				fmt.Printf("a msg send\n")
// 				cfg.one(input, 1, true)
// 			}
// 		}

// 		cfg.end()
// 	}
// }
