package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by chintan on 4/05/16.
 */
public class Message implements Serializable{

    private String senderPort;
    private String remotePort;
    private String msgType;
    private String predecessorHash;
    private String successorHash;
    private String predecessorPort;
    private String successorPort;
    private String key;
    private String value;
    private  String destination;
    private HashMap<String,String> result;


    public void setDestination(String destination) {
        this.destination = destination;
    }
    public String getDestination() {
        return destination;
    }

    public void setSenderPort(String senderPort) {
        this.senderPort = senderPort;
    }


    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getSenderPort() {
        return senderPort;
    }

    public void setRemotePort(String remotePort) {
        this.remotePort = remotePort;
    }

    public String getRemotePort() {
        return remotePort;
    }


    public void setPrePort(String predecessorPort) {
        this.predecessorPort = predecessorPort;
    }

    public void setSuccPort(String successorPort) {
        this.successorPort = successorPort;
    }

    public String getPrePort() {
        return predecessorPort;
    }

    public String getSuccPort() {
        return successorPort;
    }

    public void setPreHash(String predecessorHash) {
        this.predecessorHash = predecessorHash;
    }

    public void setSuccHash(String successorHash) {
        this.successorHash = successorHash;
    }

    public String getPreHash() {
        return predecessorHash;
    }

    public String getSuccHash() {
        return successorHash;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setResult(HashMap<String, String> result) {
        this.result = result;
    }

    public HashMap<String, String> getResult() {
        return result;
    }


}
