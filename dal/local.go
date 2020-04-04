package dal

import (
	"encoding/json"
	"errors"
	"mysql-api-server/base"
	"log"
	"net/http"
)

type LOCAL struct {
	STREAM
}

func (this *LOCAL) Unmarshal(Data []byte) error {
	err := json.Unmarshal(Data, this)
	if err != nil {
		return err
	}
	return nil
}
func (_ *LOCAL) Type() string {
	return "LOCAL"
}

func (this *LOCAL) Import(r *http.Request, SubmitImportTaskResult *(base.SubmitImportTask)) error {
	//333554432/(1024*1024) = 32MB ,最大允许上传32M的文件
	//2的20次方2²º=1048576,左移位操作表示乘法运算 1048576 * 32 = 33554432
	//r.ParseMultipartForm(32 << 24)
	file, _, err := r.FormFile("File")
	if err != nil {
		log.Println("no form file find")
		return err
	}
	defer file.Close()

	//get Cluster Database Tablename
	if this.Loadlabel == (base.LOADLABEL{}) {
		return errors.New("Load Label is null!")
	} else {
		//Load label
		if this.Loadlabel.Cluster == "" {
			return errors.New("Cluster Cannot is null!")
		}
		if this.Loadlabel.Database == "" {
			return errors.New("Database Cannot is null!")
		}
		if this.Loadlabel.Table == "" {
			return errors.New("Table Cannot is null!")
		}
		if this.Loadlabel.LabelName != "" {
			this.Loadlabel.LabelName = "LOCAL_" + this.Loadlabel.LabelName
		} else {
			this.Loadlabel.LabelName = "LOCAL_" + GetCurrentTime()
		}
	}

	//Find Be Host and Port
	Host, Port, err := GetBackendHttpPort(this.Loadlabel.Cluster)
	if err != nil {
		return err
	}
	if err = this.Local_load(file, "http://"+Host+":"+Port+"/api/"+this.Loadlabel.Database+"/"+this.Loadlabel.Table+"/_stream_load", ClusterConfig[this.Loadlabel.Cluster].User, ClusterConfig[this.Loadlabel.Cluster].Pwd, SubmitImportTaskResult); err != nil {
		return err
	}
	return nil
}
