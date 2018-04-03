package com.ftel.bigdata.radius.utils

import scalaj.http.Http

object ActiveUserUtil {
  def main(args: Array[String]) {
    val token = Http("https://210.245.0.226/rest/login")
       .postData("payload={\"username\": \"noc-mon\", \"password\": \"noc-mon\"}")
       .asString
    println(token)
  }

//  def token_ops():
//    opsview_user = "noc-mon"
//    opsview_password = "noc-mon"
//
//    payload = {
//        'username': opsview_user,
//        'password': opsview_password,
//    }
//    try:
//        token_text = requests.post('https://210.245.0.226/rest/login', data=payload, verify=False)
//        response = eval(token_text.text)
//        if "token" in response:
//            ops_token = response["token"]
//        else:
//            ops_token = None
//        return ops_token
//    except requests.exceptions.RequestException as error:
//        print error
//
//
//def get_subs(token, opsviewAddress='210.245.0.226', groupID='5', opsview_user="noc-mon"):
//    headers = {
//        "Content-Type": "application/json",
//        "X-Opsview-Username": opsview_user,
//        "X-Opsview-Token": token,
//    }
//    try:
//        result = requests.get('https://' + opsviewAddress + '/rest/status/service?hostgroupid=' + str(groupID),
//                              headers=headers, verify=False)
//        critical_dict = json.loads(result.text)
//        lst_data = []
//        for i in range(0, len(critical_dict['list'])):
//            for j in range(0, len(critical_dict['list'][i]['services'])):
//                servicename = critical_dict['list'][i]['services'][j]['name']
//                if servicename == 'Juniper MX Subscribers':
//                    output = critical_dict['list'][i]['services'][j]['output']
//                    devicename = critical_dict['list'][i]['name']
//                    ip_output = critical_dict['list'][i]['output']
//                    match_ipaddress = re.search(r"(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})", ip_output)
//                    if match_ipaddress:
//                        deviceip = match_ipaddress.group(1)
//                    lst_data.append([str(datetime.now()), deviceip, devicename, output])
//        return lst_data
//
//    except requests.exceptions.RequestException as error:
//        print error
//        return 1
}