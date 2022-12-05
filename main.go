package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/alexflint/go-arg"
)

type configuration struct {
	ConnectionString string `arg:"--connection-string,required"`

  StartBlockBytes     int `arg:"--start-block-bytes" default:"4194304"`
	EndBlockBytes       int `arg:"--end-block-bytes" default:"536870912"`
  IncrementBlockBytes int `arg:"--increment-block-bytes" default:"1048576"`

  FileSize    int `arg:"--file-size" default:"536870912"`
  Files       int `arg:"--files" default:"1"`
	Concurrency int `arg:"--concurrency" default:"1"`
}

func main() {
	cfg := configuration{}
	arg.MustParse(&cfg)
  
  opts := &container.ClientOptions{
    ClientOptions: azcore.ClientOptions{
      Retry: policy.RetryOptions{
        MaxRetries: -1,
      },
    },
  }
	containerClient, err := container.NewClientFromConnectionString(cfg.ConnectionString, fmt.Sprintf("%d", time.Now().Unix()), opts)
	if err != nil {
		log.Fatal(err)
	}
  _, err = containerClient.Create(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

  log.Println("Testing upload speed")
	err = upload(containerClient, cfg)
	if err != nil {
		log.Fatal(err)
	}
}

func upload(containerClient *container.Client, cfg configuration) error {
	for blockSize := cfg.StartBlockBytes; blockSize <= cfg.EndBlockBytes; blockSize = blockSize + cfg.IncrementBlockBytes {
		for fileCount := 1; fileCount <= cfg.Files; fileCount++ {
			fmt.Println("File Count", fileCount, "Block Size", blockSize, "File Size", cfg.FileSize)
      blobClient := containerClient.NewBlockBlobClient(fmt.Sprintf("%d-%d", blockSize, fileCount))
			opts := blockblob.UploadStreamOptions{
				BlockSize:   blockSize,
				Concurrency: cfg.Concurrency,
			}
			data := make([]byte, cfg.FileSize)
      ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
      defer cancel()
      start := time.Now()
			_, err := blobClient.UploadStream(ctx, bytes.NewReader(data), &opts)
			if err != nil {
				return err
			}
      end := time.Since(start)
			fmt.Println("File Count", fileCount, "Block Size", blockSize, "File Size", cfg.FileSize, "Duration", end)
		}
	}
	return nil
}
