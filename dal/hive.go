package dal

import (
	"encoding/json"
	"errors"
	"mysql-api-server/base"
	"net/http"
)

type HIVE struct {
	STREAM
}

func (this *HIVE) Unmarshal(Data []byte) error {
	err := json.Unmarshal(Data, this)
	if err != nil {
		return err
	}
	return nil
}
func (_ *HIVE) Type() string {
	return "HIVE"
}
func (this *HIVE) Import(r *http.Request, SubmitImportTaskResult *(base.SubmitImportTask)) error {
	if this.File == "" {
		return errors.New("Sql Cannot is null!")
	}
	if err := GetStandardClient(r, &this.StandardClient); err != nil {
		return err
	}
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
			this.Loadlabel.LabelName = "HIVE_" + this.Loadlabel.LabelName
		} else {
			this.Loadlabel.LabelName = "HIVE_" + GetCurrentTime()
		}
	}

	//Find Be Host and Port
	Host, Port, err := GetFrontendHttpPort(this.Loadlabel.Cluster)
	if err != nil {
		return err
	}
	if err = this.Hive_load(*Username, "http://"+Host+":"+Port+"/api/"+this.Loadlabel.Database+"/"+this.Loadlabel.Table+"/_hive_load", ClusterConfig[this.Loadlabel.Cluster].User, ClusterConfig[this.Loadlabel.Cluster].Pwd, SubmitImportTaskResult); err != nil {
		return err
	}
	if err = UpdateHiveLoad(this.Loadlabel); err != nil {
		return err
	}
	return nil
}
