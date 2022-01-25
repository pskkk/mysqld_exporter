package tools

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// SendReport2Users : 发送内容到指定用户的钉钉工作通知(机器人url更改)，SendReport2Users("msg_string")
func SendReport2Users(finallyRet string) {
	notNullStrReStr := "\\w"
	notNullReg := regexp.MustCompile(notNullStrReStr)
	if !notNullReg.Match([]byte(finallyRet)) {
		return
	}

	msgtext := fmt.Sprintf(`{"msgtype": "text","text": {"%s"}}`, finallyRet)

	resp, err := http.Post("https://oapi.dingtalk.com/robot/send?access_token=002789a338e2b45d686be2249d4df5e600fc78b87c33677add214afd9e205168",
		"application/json",
		strings.NewReader(msgtext))
	if err != nil {
		fmt.Println(err)
	}

	defer resp.Body.Close()
}
