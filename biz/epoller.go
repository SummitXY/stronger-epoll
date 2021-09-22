package biz

import (
	"fmt"
	"net"
	"reflect"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type Epoll struct {
	Fd int
}

var epoller *Epoll
var msec int = 0

func socketFd(conn net.Conn) int {
	tcpConn := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn")
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")
	return int(pfdVal.FieldByName("Sysfd").Int())
}

func EpollerRun() {

	fmt.Println("Epoller Run")
	ln, err := net.Listen("tcp", ":9876")
	if err != nil {
		fmt.Println("net Listen error", err)
		panic(err)
	}

	initEpoller()
	go epollerWaitRun()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("ln Accept error", err)
			panic(err)
		}
		fmt.Println("建立一个新socket")

		err = addConn2Epoll(conn)
		if err != nil {
			fmt.Println("addConn2Epoll error", err)
			continue
		}
	}
}

// private

func initEpoller() {
	epfd, err := unix.EpollCreate1(0)
	if err != nil {
		fmt.Println("unix EpollCreate1 error", err)
		panic(err)
	}

	epoller = &Epoll{
		Fd: epfd,
	}

	fmt.Println("initEpoller success fd:", epfd)
}

func addConn2Epoll(conn net.Conn) error {
	fd := socketFd(conn)
	err := unix.EpollCtl(epoller.Fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP | unix.EPOLLET, Fd: int32(fd)})
	if err != nil {
		return err
	}

	return nil
}

func removeConn4Epoll(fd int) error {
	err := unix.EpollCtl(epoller.Fd, unix.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}

	return nil
}

func epollerWaitRun() {

	for {
		events := make([]unix.EpollEvent, 128) // 每一轮处理128个socket
		n, err := unix.EpollWait(epoller.Fd, events, msec)
		if err != nil {
			fmt.Println("unix EpollWait error", err)
			continue
		}
		if n <= 0 {
			msec = -1
			fmt.Println("epoll无事件，msec设为-1")
			runtime.Gosched()
			continue
		} else {
			msec = 0
		}

		for i := 0; i < n; i++ {
			fd := events[i].Fd
			flag := events[i].Events

			if flag&unix.EPOLLRDHUP == unix.EPOLLRDHUP {
				fmt.Println("对端挂断", fd)
				err := removeConn4Epoll(int(fd))
				if err != nil {
					fmt.Println("removeConn4Epoll error", err)
				}
				continue
			}

			if flag&unix.EPOLLIN == unix.EPOLLIN {
				fmt.Println("对端可读", fd)
				size, res, err := sysRawReadV(int(fd))
				if err != nil {
					fmt.Println("sysRawReadV error", err)
					continue
				}
				// 业务逻辑新起协程
				go handler(res, size)
			}
		}
	}
}

func sysRawReadV(fd int) (int, []byte, error) {

	ivs := make([]syscall.Iovec, 2)
	buf1 := make([]byte, 10)
	ivs[0].Base = &buf1[0]
	ivs[0].Len = 10

	buf2 := make([]byte, 10)
	ivs[1].Base = &buf2[0]
	ivs[1].Len = 10

	_p0 := unsafe.Pointer(&ivs[0])

	_n, _, _e := syscall.RawSyscall(syscall.SYS_READV, uintptr(fd), uintptr(_p0), uintptr(2))
	if _e != 0 {
		return 0, []byte{}, fmt.Errorf("RawSyscall SYS_READV error:%d", _e)
	}

	res := append(buf1, buf2...)
	return int(_n), res, nil
}

func handler(content []byte, size int) {
	fmt.Println("接受到消息：", string(content))
}
