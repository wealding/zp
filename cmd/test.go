package main

import (
    "fmt"
    "time"
)

//发送者
func sender(c chan int) {
    for i := 0; i < 100; i++ {
        time.Sleep(time.Second * 11)
        c <- i
    }
}

func main(){
    ticker := time.NewTicker(10 * time.Second)
    for {
        select {
        case d := <-c:
            fmt.Println(d)
        case d := <-ticker.C:
            fmt.Println(d,"这是定时的ticker >>>>>")
        case d := <-time.NewTimer(time.Second * 3).C:
            fmt.Println(d, "这是定时的timeout *****")
        }

    }
}