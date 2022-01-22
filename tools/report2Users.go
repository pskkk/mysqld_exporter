package tools

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
)

// SendReport2Users : 发送内容到指定用户的钉钉工作通知(机器人url更改)，SendReport2Users("msg_string")
func SendReport2Users(finallyRet string) {
	notNullStrReStr := "\\w"
	notNullReg := regexp.MustCompile(notNullStrReStr)
	if !notNullReg.Match([]byte(finallyRet)) {
		return
	}

	sendMap := make(map[string]string)

	//sendMap["uname"] = strings.Join(username, ",") // post 传递过来的 就包含人名，解析出来
	sendMap["uname"] = "shimin.shan"
	sendMap["msg"] = finallyRet

	jsonByte, _ := json.Marshal(sendMap)

	request, err := http.NewRequest("POST", "https://1582641101891538.cn-beijing.fc.aliyuncs.com/2016-08-15/proxy/service-ops-helper/function-dingtalk-sendmsg/", bytes.NewBuffer(jsonByte))
	if err != nil {
		fmt.Println(err.Error())
	}

	client := http.Client{}
	_, err = client.Do(request)

	if err != nil {
		fmt.Println(err.Error())
	}
}
