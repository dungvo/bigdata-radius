PUT _template/template_radius-streaming
{
  "template": "radius-streaming-*",
  "order": 0,
  "settings": {
    "refresh_interval": "60s",
    "number_of_shards": 3,
    "number_of_replicas": 0
  },
  "mappings": {
	  "con": {
	    "properties": {
	      "type": {
	        "type": "keyword"
	      },
	      "timestamp": {
	        "type": "date",
	        "format": "date_time"
	      },
	      "session": {
	        "type": "keyword"
	      },
	      "typeLog": {
	        "type": "keyword"
	      },
	      "name": {
	        "type": "keyword"
	      },
	      "nasName": {
	        "type": "keyword"
	      },
	      "card.id": {
	        "type": "keyword"
	      },
	      "card.lineId": {
	        "type": "keyword"
	      },
	      "card.port": {
	        "type": "long"
	      },
	      "card.vlan": {
	        "type": "keyword"
	      },
	      "card.olt": {
	        "type": "keyword"
	      },
	      "cable.number": {
	        "type": "long"
	      },
	      "cable.ontId": {
	        "type": "keyword"
	      },
	      "cable.indexId": {
	        "type": "keyword"
	      },
	      "mac": {
	        "type": "keyword"
	      },
	      "vlan": {
	        "type": "keyword"
	      },
	      "serialONU": {
	        "type": "keyword"
	      },
	      "text": {
	        "type": "text"
	      }
	    }
	  },
	  "load": {
	    "properties": {
	      "type": {
	        "type": "keyword"
	      },
	      "timestamp": {
	        "type": "date",
	        "format": "date_time"
	      },
	      "statusType": {
	        "type": "keyword"
	      },
	      "nasName": {
	        "type": "keyword"
	      },
	      "nasPort": {
	        "type": "long"
	      },
	      "name": {
	        "type": "keyword"
	      },
	      "sessionID": {
	        "type": "keyword"
	      },
	      "input": {
	        "type": "long"
	      },
	      "output": {
	        "type": "long"
	      },
	      "termCode": {
	        "type": "long"
	      },
	      "sessionTime": {
	        "type": "long"
	      },
	      "ipAddress": {
	        "type": "keyword"
	      },
	      "callerID": {
	        "type": "keyword"
	      },
	      "ipv6Address": {
	        "type": "keyword"
	      },
	      "inputG": {
	        "type": "long"
	      },
	      "outputG": {
	        "type": "long"
	      },
	      "inputIPv6": {
	        "type": "long"
	      },
	      "inputIPv6G": {
	        "type": "long"
	      },
	      "outputIPv6": {
	        "type": "long"
	      },
	      "outputIPv6G": {
	        "type": "long"
	      },
	      "download": {
	        "type": "long"
	      },
	      "upload": {
	        "type": "long"
	      },
	      "contract": {
	        "type": "keyword"
	      },
	      "province": {
	        "type": "keyword"
	      },
	      "region": {
	        "type": "keyword"
	      }
	    }
	  },
	  "err": {
	    "properties": {
	      "type": {
	        "type": "keyword"
	      },
	      "timestamp": {
	        "type": "date",
	        "format": "date_time"
	      },
	      "text": {
	        "type": "long"
	      }
	    }
	  },
	  "raw": {
	    "properties": {
	      "type": {
	        "type": "keyword"
	      },
	      "timestamp": {
	        "type": "date",
	        "format": "date_time"
	      },
	      "text": {
	        "type": "long"
	      }
	    }
	  }
	}
}


PUT _template/template_radius-load
{
  "template": "radius-load-*",
  "order": 0,
  "settings": {
    "refresh_interval": "10m",
    "number_of_shards": 3,
    "number_of_replicas": 0
  },
  "mappings": {
  "docs": {
    "properties": {
      "timestamp": {
        "type": "date",
        "format": "date_time"
      },
      "name": {
        "type": "keyword"
      },
      "sessionId": {
        "type": "keyword"
      },
      "sessionTime": {
        "type": "long"
      },
      "download": {
        "type": "long"
      },
      "upload": {
        "type": "long"
      },
	      "contract": {
	        "type": "keyword"
	      },
	      "province": {
	        "type": "keyword"
	      },
	      "region": {
	        "type": "keyword"
	      }
    }
  }
}
}


PUT _template/template_monitor-radius
{
  "template": "monitor-radius-*",
  "order": 0,
  "settings": {
    "refresh_interval": "30s",
    "number_of_shards": 3,
    "number_of_replicas": 0
  },
  "mappings": {
    "docs": {
      "properties": {
        "date_time": {
          "type": "date",
          "format": "date_time"
        },
        "bras_id": {
          "type": "keyword"
        },
        "signIn": {
          "type": "double"
        },
        "logOff": {
          "type": "double"
        },
        "SL_Rate": {
          "type": "double"
        },
        "LS_Rate": {
          "type": "double"
        },
        "SL_IQR": {
          "type": "double"
        },
        "LS_IQR": {
          "type": "double"
        },
        "active_users": {
          "type": "long"
        },
        "label": {
          "type": "keyword"
        },
        "S_Active": {
          "type": "double"
        },
        "L_Active": {
          "type": "double"
        }
      }
    }
  }
}