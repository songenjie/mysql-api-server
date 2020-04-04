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
type KAFKA struct {
	Loadlabel            base.LOADLABEL       `json:"loadlabel"`
	LoadProperties       base.LOADPROPERTIES  `json:"LoadProperties"`
	JobProperties        JOBPROPERTIES        `json:"JobProperties"`
	DataSourceProperties DATASOURCEPROPERTIES `json:"DataSourceProperties"`
}

type JOBPROPERTIES struct {
	DesiredConCurrentNumber string `json:"DesiredConCurrentNumber"`
	MaxBatchInterval        string `json:"MaxBatchInterval"`
	MaxBatchRows            string `json:"MaxBatchRows"`
	MaxBatchSize            string `json:"MaxBatchSize"`
	MaxErrorNumber          string `json:"MaxErrorNumber"`
}

type DATASOURCEPROPERTIES struct {
	//必须填写
	KafkaBrokerlist string `json:"KafkaBrokerlist"`
	KafkaTopic      string `json:"KafkaTopic"`
	KafkaPartitions string `json:"KafkaPartitions"`
	KafkaOffsets    string `json:"KafkaOffsets"`

	DataType   string `json:"kafkaDataType"`
	ClientId   string `json:"ClientId"`
	GroupId    string `json:"GroupId"`
	UserName   string `json:"username"`
	PassWord   string `json:"password"`
	Datasource string `json:"datasource"`
}

func (this *KAFKA) Unmarshal(Data []byte) error {
	err := json.Unmarshal(Data, this)
	if err != nil {
		return err
	}
	return nil
}
func (_ *KAFKA) Type() string {
	return "KAFKA"

}

func (this *KAFKA) Import(_ *http.Request, _ *(base.SubmitImportTask)) error {
	Db, err := Login(this.Loadlabel.Cluster)
	if err != nil {
		return errors.New("Cannot Connect olap !" + err.Error())
	}
	defer Db.Close()

	loadstring, err := this.GetLoadString()
	if err != nil {
		return err
	}
	log.Println(loadstring)
	result, err := Db.Exec(loadstring)
	if err != nil {
		return err
	}
	_, err = result.LastInsertId()
	if err != nil {
		return err
	}
	return nil
}

func (this *KAFKA) GetLoadString() (string, error) {
	//Insert
	var LoadString strings.Builder
	//LoadLabel
	if this.Loadlabel == (base.LOADLABEL{}) {
		return "", errors.New("Load Label is null!")
	} else {
		//Load label
		if this.Loadlabel.Database == "" {
			return "", errors.New("Database Cannot is null!")
		}
		if this.Loadlabel.Table == "" {
			return "", errors.New("Database Cannot is null!")
		}
		if this.Loadlabel.LabelName != "" {
			LoadString.WriteString("CREATE ROUTINE LOAD " + this.Loadlabel.Database + "." + this.Loadlabel.LabelName + " ON " + this.Loadlabel.Table)
		} else {
			LoadString.WriteString("CREATE ROUTINE LOAD " + this.Loadlabel.Database + "." + GetCurrentTime() + " ON " + this.Loadlabel.Table)
		}
	}

	//LoadProperties
	if this.LoadProperties != (base.LOADPROPERTIES{}) {
		//column
		if this.LoadProperties.Columns != "" {
			LoadString.WriteString("\nCOLUMNS(")
			LoadString.WriteString(this.LoadProperties.Columns)
			LoadString.WriteString("\n)")
		}
		if this.LoadProperties.ColumnSeparator != "" {
			if !strings.HasSuffix(LoadString.String(), this.Loadlabel.Table) {
				LoadString.WriteString(",")
			}
			LoadString.WriteString("\nCOLUMNS TERMINATED BY \"" + this.LoadProperties.ColumnSeparator + "\"")
		}
		if this.LoadProperties.Where != "" && this.LoadProperties.Where != "*" {
			if !strings.HasSuffix(LoadString.String(), this.Loadlabel.Table) {
				LoadString.WriteString(",")
			}
			LoadString.WriteString("\nWHERE " + this.LoadProperties.Where)
		}
		if this.LoadProperties.Partitions != "" && this.LoadProperties.Partitions != "*" {
			if !strings.HasSuffix(LoadString.String(), this.Loadlabel.Table) {
				LoadString.WriteString(",")
			}
			LoadString.WriteString("\nPARTITION (" + this.LoadProperties.Partitions + ")")
		}
	}

	//all hase default value
	if this.JobProperties == (JOBPROPERTIES{}) {
		return "", errors.New("JobProperties is null!")
	} else {
		LoadString.WriteString("\nPROPERTIES")
		LoadString.WriteString("\n(")
		if this.JobProperties.DesiredConCurrentNumber != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "desired_concurrent_number", this.JobProperties.DesiredConCurrentNumber, false)
		}
		if this.JobProperties.MaxBatchInterval != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "max_batch_interval", this.JobProperties.MaxBatchInterval, false)
		}
		if this.JobProperties.MaxBatchRows != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "max_batch_rows", this.JobProperties.MaxBatchRows, false)
		}
		if this.JobProperties.MaxBatchSize != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "max_batch_size", this.JobProperties.MaxBatchSize, false)
		}
		if this.JobProperties.MaxErrorNumber != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "max_error_number", this.JobProperties.MaxErrorNumber, false)
		}
		WriteString(&LoadString, "strict_mode", "false", true)
	}

	//Datasouce
	if this.DataSourceProperties == (DATASOURCEPROPERTIES{}) {
		return "", errors.New("DataSourceProperties is null!")
	} else {
		//PROPERTIES
		if this.DataSourceProperties.DataType != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "data_type", this.DataSourceProperties.DataType, false)
		}
		LoadString.WriteString("\n)")

		//datasource
		LoadString.WriteString("\nFROM KAFKA")
		LoadString.WriteString("\n(")

		if this.DataSourceProperties.KafkaBrokerlist != "" {
			WriteString(&LoadString, "kafka_broker_list", this.DataSourceProperties.KafkaBrokerlist, false)
		} else {
			return "", errors.New("kafaka broker list cannot be null")
		}
		if this.DataSourceProperties.KafkaTopic != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "kafka_topic", this.DataSourceProperties.KafkaTopic, false)
		} else {
			return "", errors.New("kafaka Topic  cannot be null")
		}
		if this.DataSourceProperties.KafkaPartitions != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "kafka_partitions", this.DataSourceProperties.KafkaPartitions, false)
		}
		if this.DataSourceProperties.KafkaOffsets != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "kafka_offsets", this.DataSourceProperties.KafkaOffsets, false)
		}
		if this.DataSourceProperties.ClientId != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "property.client.id", this.DataSourceProperties.ClientId, false)
		}
		if this.DataSourceProperties.GroupId != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "property.group.id", this.DataSourceProperties.GroupId, false)
		}
		if this.DataSourceProperties.DataType != "" {
			if !strings.HasSuffix(LoadString.String(), "(") {
				LoadString.WriteString(",")
			}
			WriteString(&LoadString, "property.data_type", this.DataSourceProperties.DataType, false)
		}

		if strings.EqualFold(this.DataSourceProperties.Datasource, "jdq") {
			WriteString(&LoadString, "property.datasource", "jdq", true)
		} else if strings.EqualFold(this.DataSourceProperties.Datasource, "jmq") {
			WriteString(&LoadString, "property.datasource", "jmq", true)
			WriteString(&LoadString, "property.security.protocol", "PLAINTEXT", true)
		} else if strings.EqualFold(this.DataSourceProperties.Datasource, "dcs") {
			WriteString(&LoadString, "property.datasource", "dcs", true)
			WriteString(&LoadString, "property.bootstrap.servers", this.DataSourceProperties.KafkaBrokerlist, true)
			WriteString(&LoadString, "property.sasl.username", this.DataSourceProperties.UserName, true)
			WriteString(&LoadString, "property.sasl.password", this.DataSourceProperties.PassWord, true)
			WriteString(&LoadString, "property.isolation.level", "read_committed", true)
			WriteString(&LoadString, "property.key.serializer", "serialization.StringDeseri¬¬alizer", true)
			WriteString(&LoadString, "property.value.serializer", "serialization.StringDeserializer", true)
			WriteString(&LoadString, "property.sasl.mechanisms", "SCRAM-SHA¬-256", true)
			WriteString(&LoadString, "property.", "SASL_PLAINTEXT", true)
		} else {
			return "", errors.New(this.DataSourceProperties.Datasource + " not improve !")
		}
		LoadString.WriteString("\n)")
	}

	LoadString.WriteString(";\n")

	return LoadString.String(), nil
}
