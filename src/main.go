package main

import (
	//"container/list"
	"fmt"
)

type MyStruct struct {
	S string
	I int
}

func main() {
	// var dict map[int]string = make(map[int]string)
	// dict[0] = "Zero"
	// dict[5] = "Five"
	// if val, isIn := dict[1]; !isIn {
	// 	fmt.Println(val)
	// }
	// if val, isIn := dict[0]; isIn {
	// 	fmt.Println(val)
	// }
	// for key, val := range dict {
	// 	fmt.Printf("key: %d\tval: %s\n", key, val)
	// }
	// 	l := list.New()
	// 	l.PushBack(MyStruct{S: "Hi", I: 5})
	// 	elem := l.Back()
	// 	fmt.Println(MyStruct(elem.Value.(MyStruct)).I, MyStruct(elem.Value.(MyStruct)).S)
	fmt.Println(string([]byte{49, 50, 55, 46, 48, 46, 48, 46, 49}))
}
