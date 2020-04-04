package dal

import (
	"encoding/json"
	"io/ioutil"
	"mysql-api-server/base"
	"log"
	"net/http"
)

//Cluster 提前关联了  这里只需要传入Database
func ModificationStatus(re *(base.BaseHttpResult), Modification base.MODIFICATION) {
	var ExecString = ""
	//Stream Load  curl -u user:passwd -XPOST http://host:port/api/{db}/{label}/_cancel

	if Modification.ImportType == "HDFS" {
		//SUSPENDCannot
		if Modification.Action == "SUSPEND" || Modification.Action == "STOP" {
			ExecString = "CANCEL LOAD  FROM " + Modification.Database + " WHERE LABEL = \"" + Modification.Label + "\";"
		} else {
			re.Message = Modification.ImportType + " " + Modification.Action + "Cannot use "
			return
		}

	} else if Modification.ImportType == "KAFKA" {
		//Load Cluster Olap
		//SUSPEND PAUSE
		if Modification.Action == "SUSPEND" || Modification.Action == "PAUSE" {
			ExecString = "PAUSE ROUTINE LOAD FOR " + Modification.Label + ";"
		} else if Modification.Action == "STOP" {
			ExecString = "STOP  ROUTINE LOAD FOR " + Modification.Label + ";"
		} else if Modification.Action == "RESUME" {
			ExecString = "RESUME  ROUTINE LOAD FOR " + Modification.Label + ";"
		} else {
			re.Message = Modification.ImportType + " " + Modification.Action + "Cannot use "
			return
		}

	} else if Modification.ImportType == "LOCAL" || Modification.ImportType == "HIVE" {
		if Modification.Action == "STOP" {
			Host, Port, err := GetFrontendHttpPort(Modification.Cluster)
			if err != nil {
				re.Message = err.Error()
				return
			}
			url := "http://" + Host + ":" + Port + "/api/" + Modification.Database + "/" + Modification.Label + "/_cancel"

			client := &http.Client{}
			req, err := http.NewRequest("POST", url, nil)
			if err != nil {
				re.Message = err.Error()
				return
			}
			req.SetBasicAuth("root", "")

			resp, err := client.Do(req)
			if err != nil {
				re.Message = err.Error()
				return
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				re.Message = err.Error()
				return
			}
			log.Println(string(body))
			err = json.Unmarshal(body, re)
			if err != nil {
				re.Message = err.Error()
				return
			}
			return
		} else {
			re.Message = Modification.ImportType + " " + Modification.Action + "Cannot use "
			return
		}
	} else {
		re.Message = Modification.ImportType + "Not Exist"
		return
	}

	Db, err := Login(Modification.Cluster)
	if err != nil {
		re.Message = "Cannot Connect olap !" + err.Error()
		return
	}
	defer Db.Close()
	log.Println(ExecString)
	result2, err := Db.Exec("use " + Modification.Database + ";")
	if err != nil {
		re.Message = err.Error()
		return
	}
	_, err = result2.LastInsertId()
	if err != nil {
		re.Message = err.Error()
		return
	}
	result, err := Db.Exec(ExecString)
	if err != nil {
		re.Message = err.Error()
		return
	}
	_, err = result.LastInsertId()
	if err != nil {
		re.Message = err.Error()
		return
	}
	re.Code = http.StatusOK
	re.Message = "Success"
}
