package backup

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/codeskyblue/go-sh"
	"github.com/pkg/errors"

	"github.com/stefanprodan/mgob/pkg/config"
)

func s3Upload(file string, plan config.Plan) (string, error) {

	if len(plan.S3.AccessKey) > 0 && len(plan.S3.SecretKey) > 0 {
		configure := fmt.Sprintf("aws configure set aws_access_key_id %v && aws configure set aws_secret_access_key %v",
			plan.S3.AccessKey, plan.S3.SecretKey)

		result, err := sh.Command("/bin/sh", "-c", configure).CombinedOutput()
		output := ""
		if len(result) > 0 {
			output = strings.Replace(string(result), "\n", " ", -1)
		}
		if err != nil {
			return "", errors.Wrapf(err, "aws configure for plan %v failed %s", plan.Name, output)
		}
	}

	fileName := filepath.Base(file)

	upload := fmt.Sprintf("aws --quiet s3 cp %v s3://%v/%v",
		file, plan.S3.Bucket, fileName)

	result, err := sh.Command("/bin/sh", "-c", upload).SetTimeout(time.Duration(plan.Scheduler.Timeout) * time.Minute).CombinedOutput()
	output := ""
	if len(result) > 0 {
		output = strings.Replace(string(result), "\n", " ", -1)
	}

	if err != nil {
		return "", errors.Wrapf(err, "S3 uploading %v to %v/%v failed %v", file, plan.Name, plan.S3.Bucket, output)
	}

	if strings.Contains(output, "<ERROR>") {
		return "", errors.Errorf("S3 upload failed %v", output)
	}

	return strings.Replace(output, "\n", " ", -1), nil
}
