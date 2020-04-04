package dal

import (
	"encoding/json"
	"errors"
	"mysql-api-server/base"
	"log"
	"net/http"
	"strings"
)

//broker info
type HDFS struct {
	Loadlabel      base.LOADLABEL      `json:"loadlabel"`
	Desc           []*DESC             `json:"desc"`
	BrokerName     string              `json:"brokername"`
	Namespace      string              `json:"namespace"`
	Properties     base.PROPERTIES     `json:"properties"`
	StandardClient base.STANDARDCLIENT `json:"StandardClient"`
}

//hdfs info
type DESC struct {
	FilePath       string              `json:"filepath"`
	Table          string              `json:"table"`
	Negative       string              `json:"negative"`
	FileType       string              `json:"filetype"`
	LoadProperties base.LOADPROPERTIES `json:"loadproperties"`
	Set            string              `json:"set"`
}

func (this *HDFS) Unmarshal(Data []byte) error {
	err := json.Unmarshal(Data, this)
	if err != nil {
		return err
	}
	return nil
}
func (_ *HDFS) Type() string {
	return "HDFS"
}
func (this *HDFS) init() {

}

func (this *HDFS) Import(r *http.Request, SubmitImportTaskResult *(base.SubmitImportTask)) error {
	if err := GetStandardClient(r, &this.StandardClient); err != nil {
		return err
	}
	//Insert
	var LoadString strings.Builder
	if this.Loadlabel != (base.LOADLABEL{}) {
		//Load label
		if this.Loadlabel.Cluster == "" {
			return errors.New("Cluster Cannot is null!")
		}
		if this.Loadlabel.Database == "" {
			return errors.New("Database Cannot is null!")
		}
		if this.Loadlabel.LabelName != "" {
			LoadString.WriteString("LOAD LABEL " + this.Loadlabel.Database + "." + this.Loadlabel.LabelName)
		} else {
			LoadString.WriteString("LOAD LABEL " + this.Loadlabel.Database + "." + GetCurrentTime())
		}
	} else {
		return errors.New("Load Label is null!")
	}

	if len(this.Desc) == 0 {
		return errors.New("Desc is null!")
	} else {
		LoadString.WriteString("\n(")
		for i, desc := range this.Desc {
			//Filepath
			log.Println(desc.FilePath)
			log.Println(this.StandardClient.Production)
			if desc.FilePath == "" {
				return errors.New("FilePath Cannot is null!")
			} else if !strings.Contains(desc.FilePath, this.StandardClient.Production) {
				return errors.New(desc.FilePath + " is not in  " + this.StandardClient.Market + "." + this.StandardClient.Production + "!")
			} else {
				if i > 0 {
					LoadString.WriteString(",")
				}
				LoadString.WriteString("\nDATA INFILE(\"" + desc.FilePath + "\")")
				this.Namespace = strings.Split(strings.Split(desc.FilePath, "//")[1], "/")[0]
			}
			//table
			if desc.Table == "" {
				return errors.New("Table Cannot is null!")
			} else {
				LoadString.WriteString("\nINTO TABLE `" + desc.Table + "`")
			}
			//Partition
			if desc.LoadProperties.Partitions != "" {
				LoadString.WriteString("\nPARTITION (" + desc.LoadProperties.Partitions + ")")
			}
			//if Netgative
			if desc.Negative == "1" {
				LoadString.WriteString("\nNEGATIVE")
			}
			//column
			if desc.LoadProperties.ColumnSeparator != "" {
				LoadString.WriteString("\nCOLUMNS TERMINATED BY \"" + desc.LoadProperties.ColumnSeparator + "\"")
			}
			//filetype
			if desc.FileType != "" {
				LoadString.WriteString("\nFORMAT AS \"" + desc.FileType + "\"")
			}
			if desc.LoadProperties.Columns != "" {
				LoadString.WriteString("\n(" + desc.LoadProperties.Columns + ")")
			}
			//set
			if desc.Set != "" {
				LoadString.WriteString("\nSET")
				LoadString.WriteString("\n(" + desc.Set)
				LoadString.WriteString("\n)")
			}
		}
		LoadString.WriteString("\n)")
	}

	//broker
	this.init()
	//LoadString.WriteString("\nWITH BROKER " + this.BrokerName)
	LoadString.WriteString("\nWITH BROKER " + *Brokername)
	LoadString.WriteString("\n(")

	//namespace
	LoadString.WriteString("\n\"dfs.nameservices\"=\"" + this.Namespace + "\",")
	btrue := 0
	for _, hdfsconfig := range HdfsConfig {
		if hdfsconfig.Nameservices == this.Namespace {
			LoadString.WriteString("\n\"dfs.ha.namenodes." + this.Namespace + "\"=\"" + hdfsconfig.Namenodes[0].Name + "," + hdfsconfig.Namenodes[1].Name + "\",")
			LoadString.WriteString("\n\"dfs.namenode.rpc-address." + this.Namespace + "." + hdfsconfig.Namenodes[0].Name + "\"=\"" + hdfsconfig.Namenodes[0].Rpc_address + "\",")
			LoadString.WriteString("\n\"dfs.namenode.rpc-address." + this.Namespace + "." + hdfsconfig.Namenodes[1].Name + "\"=\"" + hdfsconfig.Namenodes[1].Rpc_address + "\",")
			btrue = 1
		}
	}
	if btrue == 0 {
		return errors.New("FilePath Cannot is null!")
	}

	LoadString.WriteString("\n\"dfs.client.failover.proxy.provider\"=\"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\"")
	LoadString.WriteString("\n)")

	//Properties
	if this.Properties != (base.PROPERTIES{}) {
		LoadString.WriteString("\nPROPERTIES")
		LoadString.WriteString("\n(")
		if this.Properties.Timeout != "" {
			LoadString.WriteString("\n\"timeout\" = \"" + this.Properties.Timeout + "\"")
		}
		if this.Properties.MaxFilterRatio != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			LoadString.WriteString("\n\"max_filter_ratio\" = \"" + this.Properties.MaxFilterRatio + "\"")
		}
		LoadString.WriteString("\n)")
	}

	LoadString.WriteString(";\n")
	log.Println(LoadString.String())
	Db, err := Login(this.Loadlabel.Cluster)
	if err != nil {
		return errors.New("Cannot Connect olap !" + err.Error())
	}
	defer Db.Close()

	result, err := Db.Exec(LoadString.String())
	if err != nil {
		return err
	}
	_, err = result.LastInsertId()
	if err != nil {
		return err
	}
	return nil
}
