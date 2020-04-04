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

func ModificationStatus(w http.ResponseWriter, r *http.Request) {
	//set default result
	re := base.BaseHttpResult{
		Code:    http.StatusMethodNotAllowed,
		Message: "failed",
	}
	Modification := base.MODIFICATION{}

	defer func() {
		w.Header().Set("Content-Type", "application/json")
		log.Println("ModificationStatus ", re)
		res, _ := json.Marshal(re)
		io.WriteString(w, string(res))
	}()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		re.Message = err.Error()
		return
	}

	if err := json.Unmarshal(body, &Modification); err != nil {
		re.Message = err.Error()
		return
	}

	if Modification.ImportType == "" || Modification.Cluster == "" || Modification.Database == "" || Modification.Action == "" {
		re.Message = "parmeter is not complete"
		return
	}

	dal.ModificationStatus(&re, Modification)
}
