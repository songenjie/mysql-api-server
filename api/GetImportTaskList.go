package api

import (
	"encoding/json"
	"io"
	"mysql-api-server/base"
	"mysql-api-server/dal"
	"log"
	"net/http"
	"strconv"
)

func GetImportTaskList(w http.ResponseWriter, r *http.Request) {
	//set default result
	re := base.ImportTaskListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "Success",
		},
	}
	defer func() {
		w.Header().Set("Content-Type", "application/json")
		log.Println("GetImportTaskList ", re)
		res, _ := json.Marshal(re)
		io.WriteString(w, string(res))
	}()

	v := r.URL.Query()
	if v.Get("Index") == "" {
		re.Message = "Index  Cannot is null!"
		return
	}
	if v.Get("PageSize") == "" {
		re.Message = "PageSize  Cannot is null!"
		return
	}
	Index, err := strconv.Atoi(v.Get("Index"))
	if err != nil {
		re.Message = err.Error()
		return
	}
	PageSize, err := strconv.Atoi(v.Get("PageSize"))
	if err != nil {
		re.Message = err.Error()
		return
	}

	ListDefine := base.LISTDEFINE{
		Index:    Index,
		PageSize: PageSize,
	}

	if result := dal.GetUsernameBysso(r); result.Message != "Success" {
		re.Message = result.Message
		re.Code = result.Code
		return
	}
	dal.GetImportTaskList(&re, ListDefine, *dal.Username)
}
