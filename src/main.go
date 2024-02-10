package main

import "fmt"

func main() {
	var dict map[int]string = make(map[int]string)
	dict[0] = "Zero"
	dict[5] = "Five"
	if val, isIn := dict[1]; !isIn {
		fmt.Println(val)
	}
	if val, isIn := dict[0]; isIn {
		fmt.Println(val)
	}
	for key, val := range dict {
		fmt.Printf("key: %d\tval: %s\n", key, val)
	}
}
