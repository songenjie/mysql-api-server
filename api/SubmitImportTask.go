package api

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"mysql-api-server/base"
	"mysql-api-server/dal"
	"log"
	"net/http"
)

func SubmitImportTask(w http.ResponseWriter, r *http.Request) {
	re := base.SubmitImportTaskResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "Failed",
		},
	}
	defer func() {
		w.Header().Set("Content-Type", "application/json")
		log.Println("SubmitImportTask ", re)
		res, _ := json.Marshal(re)
		io.WriteString(w, string(res))
	}()

	var Olap base.OLAP
	//333554432/(1024*1024) = 32MB ,最大允许上传32M的文件
	//2的20次方2²º=1048576,左移位操作表示乘法运算 1048576 * 32 = 33554432
	r.ParseMultipartForm(32 << 24)
	log.Println(*(r.MultipartForm))
	file, _, err := r.FormFile("Data")
	if err != nil {
		re.Message = err.Error()
		return
	}
	body, err := ioutil.ReadAll(file)
	if err != nil {
		re.Message = err.Error()
		return
	}

	if err := json.Unmarshal(body, &Olap); err != nil {
		re.Message = err.Error()
		return
	}

	ImportObject, err := dal.GetImportObject(Olap.ImportType)
	if err != nil {
		re.Message = err.Error()
		return
	}

	Olap.Data = ImportObject
	if err = json.Unmarshal(body, &Olap); err != nil {
		re.Message = err.Error()
		return
	}

	log.Println(ImportObject)
	err = ImportObject.Import(r, &(re.Data))
	if err != nil {
		re.Message = err.Error()
		return
	}
	re.Code = http.StatusOK
	re.Message = "Success"
}
