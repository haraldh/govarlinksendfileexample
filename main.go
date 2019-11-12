package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/haraldh/govarlinksendfileexample/comexamplesendfile"
	"github.com/varlink/go/varlink"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

type server struct {
	comexamplesendfile.VarlinkInterface
	cacheDir string
}

func checkFileHandle(handle string) (err error) {
	err = nil

	if strings.Contains(handle, "/") {
		err = fmt.Errorf("Invalid File Handle")
	}
	return
}

func (m *server) SendFile(ctx context.Context, c comexamplesendfile.VarlinkCall, type_ string, length int64) (err error) {

	if !c.WantsUpgrade() {
		err = fmt.Errorf("Client did not set Upgrade for SendFile")
		return
	}

	if length > 10000000 {
		err = c.ReplyErrorFileTooBig(ctx)
		return
	}

	// Generate random file name in cache directory
	file_handle, err := ioutil.TempDir(m.cacheDir, "sendfile")

	if err != nil {
		fmt.Printf("Error: TempDir() %v\n", err)
		err = c.ReplyErrorFileCreate(ctx)
		return
	}

	if err = c.ReplySendFile(ctx, path.Base(file_handle)); err != nil {
		return
	}

	// Connection is now upgraded and cannot be used for the varlink protocol anymore

	fs, err := os.Create(path.Join(file_handle, "file"))
	if err != nil {
		fmt.Printf("Error: SendFile() Create() %v\n", err)
		return
	}
	defer fs.Close()

	writer := bufio.NewWriter(fs)

	buf4096 := make([]byte, 4096)
	var buf []byte

	toBeCopied := int(length)

	for toBeCopied > 0 {
		if toBeCopied >= 4096 {
			buf = buf4096
		} else {
			buf = make([]byte, toBeCopied)
		}

		numBytesRead, err := c.Conn.Read(ctx, buf)

		if numBytesRead == toBeCopied {
			err = nil
		}

		if err != nil {
			fmt.Printf("Error: SendFile() Read() %v %d %d\n", err, numBytesRead, toBeCopied)
			break
		}

		if numBytesRead == 0 {
			fmt.Printf("Error: SendFile() numBytesRead == 0\n")
			break
		}

		toBeCopied = toBeCopied - numBytesRead

		numBytesToWrite := numBytesRead

		for i := 0; numBytesToWrite > 0;  {
			w, err := writer.Write(buf[i:(i + numBytesToWrite)])
			if err != nil {
				fmt.Printf("Error: SendFile() Write() %v\n", err)
			}
			i = i + w
			numBytesToWrite = numBytesToWrite - w
		}

		if numBytesToWrite > 0 || err != nil {
			break
		}
	}
	writer.Flush()
	fs.Close()

	if toBeCopied > 0 || err != nil {
		os.Remove(file_handle)
		return
	}

	buf = make([]byte, 1)
	buf[0] = 0xa
	// Send an ACK to the client
	c.Call.Conn.Write(ctx, buf)

	// Connection is upgraded and cannot be used for the varlink protocol anymore
	// Close the connection
	return fmt.Errorf("SendFile done")
}

func (m *server) DeleteFile(ctx context.Context, c comexamplesendfile.VarlinkCall, file_handle string) (err error) {

	if err = checkFileHandle(file_handle); err != nil {
		err = c.ReplyErrorInvalidFileHandle(ctx)
		return
	}

	if _, err = os.Stat(path.Join(m.cacheDir, file_handle)); err != nil {
		err = c.ReplyErrorInvalidFileHandle(ctx)
		return
	}

	err = os.RemoveAll(path.Join(m.cacheDir, file_handle))
	if err != nil {
		return
	}

	err = c.ReplyDeleteFile(ctx)
	return
}

func (m *server) LsFile(ctx context.Context, c comexamplesendfile.VarlinkCall, file_handle string) (err error) {
	if err = checkFileHandle(file_handle); err != nil {
		err = c.ReplyErrorInvalidFileHandle(ctx)
		return
	}

	fs, err := os.Open(path.Join(m.cacheDir, file_handle, "file"))
	if err != nil {
		fmt.Printf("Error: LsFile() Open() %v\n", err)
		return
	}

	fi, err := fs.Stat()
	if err != nil {
		fmt.Printf("Error: LsFile() Stat() %v\n", err)
		return
	}

	err = c.ReplyLsFile(ctx, comexamplesendfile.FileAttributes{Size: fi.Size()})
	return
}

func help() {
	name := os.Args[0]
	fmt.Printf("Server Usage: %s --varlink <varlink address URL>\n", name)
	fmt.Printf("Client Usage: %s --varlink <varlink address URL> --client <FILE>\n", name)
	fmt.Println()
	fmt.Println("E.g:")
	fmt.Printf("  %s --varlink unix:/tmp/sendfile\n", name)
	fmt.Printf("  %s --varlink unix:/tmp/sendfile --client main.go\n", name)
	os.Exit(1)
}

func run_server(address string) (err error) {
	var service *varlink.Service

	homedir := os.Getenv("HOME")
	m := server{cacheDir: path.Join(homedir, ".cache", "sendfile")}

	err = os.MkdirAll(m.cacheDir, 0755)
	if err != nil {
		return
	}

	// May want to prune the .cache dir of old directories here

	service, _ = varlink.NewService(
		"Varlink",
		"Send File Example",
		"1",
		"https://github.com/haraldh/govarlinksendfileexample",
	)

	err = service.RegisterInterface(comexamplesendfile.VarlinkNew(&m))

	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = service.Listen(ctx, address, time.Duration(60)*time.Second)

	return
}

func run_client(address string, filename string) (err error) {
	fs, err := os.Open(filename)
	if err != nil {
		return
	}

	fi, err := fs.Stat()
	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var c *varlink.Connection

	c, err = varlink.NewConnection(ctx, address)
	if err != nil {
		fmt.Println("Failed to connect")
		return
	}
	defer c.Close()

	// We have to use Send() to send the varlink.Upgrade flag
	receive, err := comexamplesendfile.SendFile().Upgrade(ctx, c, "file", int64(fi.Size()))
	if err != nil {
		fmt.Println("SendFile() call failed")
		return
	}
	file_handle, _, rw_cont, err := receive(ctx)
	if err != nil {
		fmt.Println("SendFile() call failed")
		return
	}

	fmt.Printf("Got FileHandle: %v\n", file_handle)

	// We received the FileHandle, which means, that the connection is now upgraded
	// and we talk our own protocol

	reader := bufio.NewReader(fs)
	buf := make([]byte, 2048)
	for {
		n, r_err := reader.Read(buf)
		if r_err != nil || n == 0 {
			break
		}
		_, w_err := rw_cont.Write(ctx, buf)
		if w_err != nil {
			fmt.Println("WriteTo() failed")
			return
		}
	}

	buf = make([]byte, 1)
	// All was sent, wait for the ACK from the server
	b, err := rw_cont.Read(ctx, buf)
	if b != 0 {
		fmt.Printf("Ret from Server: %v\n", buf)
	}

	// Because the connection is upgraded, we cannot reuse the connection for the varlink
	// protocol anymore. Close it and reopen the connection.
	c.Close()
	c, err = varlink.NewConnection(ctx, address)
	if err != nil {
		fmt.Println("Failed to connect again")
		return
	}

	stat, err := comexamplesendfile.LsFile().Call(ctx, c, file_handle)
	if err != nil {
		fmt.Println("LsFile() failed")
		return
	}
	fmt.Printf("Got FileAttributes: Size=%v\n", stat.Size)

	err = comexamplesendfile.DeleteFile().Call(ctx, c, file_handle)
	if err != nil {
		fmt.Println("DeleteFile() failed")
		return
	}
	fmt.Printf("Removed %v\n", file_handle)

	err = comexamplesendfile.DeleteFile().Call(ctx, c, file_handle)
	if err != nil {
		switch err.(type) {
		case *comexamplesendfile.ErrorInvalidFileHandle:
			// Calling DeleteFile() a second time fails of course
			err = nil
		default:
			// Report the other error
			return
		}
	} else {
		err = fmt.Errorf("Deleting a deleted file should not succeed")
	}

	return
}

func main() {
	var address string
	var client bool

	flag.Usage = help
	flag.StringVar(&address, "varlink", "", "Varlink address")
	flag.BoolVar(&client, "client", false, "Run as client")
	flag.Parse()

	if address == "" {
		help()
		os.Exit(1)
	}

	if client {
		if len(flag.Args()) != 1 {
			help()
			os.Exit(1)
		}

		err := run_client(address, flag.Arg(0))
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
		return
	}

	err := run_server(address)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	return
}
