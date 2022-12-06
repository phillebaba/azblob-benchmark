package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/alexflint/go-arg"
	units "github.com/docker/go-units"
)

type configuration struct {
	ConnectionString string `arg:"--connection-string,required"`

	StartBlockBytes     int `arg:"--start-block-bytes" default:"2097152"`
	EndBlockBytes       int `arg:"--end-block-bytes" default:"536870912"`
	IncrementBlockBytes int `arg:"--increment-block-bytes" default:"2097152"`

	FileSize    int `arg:"--file-size" default:"536870912"`
	Files       int `arg:"--files" default:"1"`
	Concurrency int `arg:"--concurrency" default:"1"`

	CSVFilePath string `arg:"--csv-file-path,required"`
}

func main() {
	cfg := configuration{}
	arg.MustParse(&cfg)

	// Create Azure clients
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
	defer func(containerClient *container.Client) {
		_, err := containerClient.Delete(context.Background(), nil)
		if err != nil {
			log.Fatal(err)
		}
	}(containerClient)

	// Get measurement data
	out, err := measure(containerClient, cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Write output CSV data
	csvFile, err := os.Create(cfg.CSVFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()
	csvwriter := csv.NewWriter(csvFile)
	for _, row := range out {
		err = csvwriter.Write(row)
		if err != nil {
			log.Fatal(err)
		}
	}
	csvwriter.Flush()
}

func measure(containerClient *container.Client, cfg configuration) ([][]string, error) {
	out := [][]string{
		{"Block Size", "Upload Duration", "Download Duration"},
	}

	for blockSize := cfg.StartBlockBytes; blockSize <= cfg.EndBlockBytes; blockSize = blockSize + cfg.IncrementBlockBytes {
		for fileCount := 1; fileCount <= cfg.Files; fileCount++ {
			blobClient := containerClient.NewBlockBlobClient(fmt.Sprintf("%d-%d", blockSize, fileCount))
			opts := blockblob.UploadStreamOptions{
				BlockSize:   blockSize,
				Concurrency: cfg.Concurrency,
			}
			fileSizeHuman := units.BytesSize(float64(cfg.FileSize))
			blockSizeHuman := units.BytesSize(float64(blockSize))

			// Upload File
			data := make([]byte, cfg.FileSize)
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			startUpload := time.Now()
			_, err := blobClient.UploadStream(ctx, bytes.NewReader(data), &opts)
			if err != nil {
				return nil, err
			}
			uploadDuration := time.Since(startUpload)
			fmt.Println("Upload", "File Count", fileCount, "Block Size", blockSizeHuman, "File Size", fileSizeHuman, "Duration", uploadDuration)

			// Download File
			ctx, cancel = context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			startDownload := time.Now()
			resp, err := blobClient.DownloadStream(ctx, nil)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			_, err = io.Copy(io.Discard, resp.Body)
			if err != nil {
				return nil, err
			}
			downloadDuration := time.Since(startDownload)
			fmt.Println("Download", "File Count", fileCount, "Block Size", blockSizeHuman, "File Size", fileSizeHuman, "Duration", downloadDuration)

			// Write measured data
			out = append(out, []string{blockSizeHuman, strconv.FormatInt(uploadDuration.Milliseconds(), 10), strconv.FormatInt(downloadDuration.Milliseconds(), 10)})
		}
	}
	return out, nil
}
