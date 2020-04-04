package api

import (
	"encoding/json"
	"io"
	"mysql-api-server/base"
	"mysql-api-server/dal"
	"log"
	"net/http"
)

func ExportTask(w http.ResponseWriter, r *http.Request) {
	//set default result
	re := base.RoutineLoadResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	defer func() {
		w.Header().Set("Content-Type", "application/json")
		log.Println("ExportTask: ", re.Routineload)
		res, _ := json.Marshal(re)
		io.WriteString(w, string(res))
	}()

	if result := dal.GetUsernameBysso(r); result.Message != "Success" {
		re.Message = result.Message
		re.Code = result.Code
		return
	}
	var ifContain bool
	for _, user := range dal.AdminUserConfig {
		log.Println(*user)
		log.Println(*dal.Username)
		if *user == *dal.Username {
			ifContain = true
		}
	}
	if !ifContain {
		re.Message = *dal.Username + " is not admin user"
		return
	}
	dal.ExportTask(&re)
}
