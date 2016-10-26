package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.os.AsyncTask;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;
import java.net.InetAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDhtProvider extends ContentProvider {

    final static String TAG = SimpleDhtProvider.class.getSimpleName();
    String myPort;
    String predecessorHash;
    String successorHash;
    String predecessorPort;
    String successorPort;
    String[] mapKeys;
    static final int SERVER_PORT = 10000;
    private boolean all_reply_flag = false;
    private boolean single_reply_flag = false;
    HashMap<String, String> result;
    TreeMap<String, String> tMap;
    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        map.remove(selection);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            String keyHash = genHash((String) values.get("key"));
            String key = (String) values.get("key");
            String value = (String) values.get("value");
            String ownHash = genHash(Integer.toString(Integer.parseInt(myPort) / 2));
            // 3 conditions need to be checked
            // First Check If there is only one node
            if (ownHash.equals(predecessorHash)) {
                Log.i(TAG, "one node only");
                map.put(key, value);
            }
            // Second Check If this is the first node
            else if (predecessorHash.compareTo(ownHash) > 0) {
                Log.i(TAG, "First Node in the chord");
                // If this node is first then check if the keyHash is greater than both predecessorHash and ownHash
                if (keyHash.compareTo(predecessorHash) > 0 && keyHash.compareTo(ownHash) > 0) {
                    map.put(key, value);
                }
                //If this node is first then check if the keyHash is lesser than predecessorHash and lesser than equal to ownHash
                else if (keyHash.compareTo(predecessorHash) < 0 && keyHash.compareTo(ownHash) <= 0) {
                    map.put(key, value);
                }
                else {
                    // Forward it to the successorPort
                    Message m = new Message();
                    m.setRemotePort(successorPort);
                    m.setKey((String) values.get("key"));
                    m.setValue((String) values.get("value"));
                    m.setMsgType("ForwardInsertRequest");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
                    Log.i(TAG, "This is First Node. Forwarded to the successorPort: " + successorPort);
                }
            }
            //Third check If the keyHash is greater than predecessorHash but less than or equal to ownHash
            else if (keyHash.compareTo(ownHash) <= 0 && keyHash.compareTo(predecessorHash) > 0) {
                Log.i(TAG, "Between Predecessor and ownHash");
                map.put(key, value);
            } else {
                // Forward it to the successorPort
                Message m = new Message();
                m.setKey((String) values.get("key"));
                m.setValue((String) values.get("value"));
                m.setMsgType("ForwardInsertRequest");
                m.setRemotePort(successorPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
                Log.i(TAG, "Forwarded to the successorPort: " + successorPort);
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.i(TAG, "Node " + myPort + " created.");
        try {
            String ownHash = genHash(Integer.toString(Integer.parseInt(myPort) / 2));
            predecessorHash = ownHash;
            successorHash = ownHash;
            predecessorPort = myPort;
            successorPort = myPort;
            Log.i(TAG, "myPort: " + myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        tMap = new TreeMap<>();

        // Server created
        try {
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, new ServerSocket(SERVER_PORT));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //If port created is 5554 then no need to send the join request to anyone as this is the first port which is created
        if (myPort.equals("11108")) {
            try {
                Log.i(TAG, "Node Joined: 11108");
                String hash = genHash("5554");
                tMap.put(hash, "5554");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        } else {
            // Create a message with MsgType "JoinRequest" and send it to port 5554 to join your port to the chord.
            Message m = new Message();
            m.setMsgType("JoinRequest");
            m.setSenderPort(myPort);
            m.setRemotePort("11108");
            Log.i(TAG, "Sent a JoinRequest to 5554");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
        }
        return true;

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        String[] cols = {"key", "value"};
        MatrixCursor cur = new MatrixCursor(cols);
        String queryKey = selection;
        switch (queryKey) {
            case "*":
                try {
                    Log.v("query", selection);
                    String ownHash = genHash(Integer.toString(Integer.parseInt(myPort) / 2));
                    Log.i(TAG, "Current Port is " + myPort);
                    //check if there is only one node
                    if (predecessorHash.compareTo(ownHash) == 0) {
                        Log.v(TAG, "There is only one node");
                        for (Map.Entry<String, String> me : map.entrySet()) {
                            cur.addRow(new String[]{me.getKey(), me.getValue()});
                        }
                    }
                    else {
                        // Forward a msg to the successorPort requesting to get all messages stored at that port and wait till you get all reply
                        HashMap<String, String> result2 = new HashMap<>();
                        {
                            for (Map.Entry<String, String> me : map.entrySet()) {
                                Log.i(TAG, " Entryvalues start  " + me.getKey() + " " + me.getValue());
                                result2.put(me.getKey(), me.getValue());
                            }
                        }
                        Message m = new Message();
                        m.setResult(result2);
                        m.setMsgType("ForwardGetQueriesRequest");
                        m.setSenderPort(myPort);
                        m.setRemotePort(successorPort);
                        {
                            Log.i(TAG, "Forward the query request successorPort " + m.getRemotePort());
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
                        }
                        //wait till you get the replies
                        while(true)
                        {
                            if(all_reply_flag)
                            {
                                all_reply_flag = false;
                                break;
                            }
                        }
                        Log.i(TAG, "Received replies.");
                        MatrixCursor matrixCur = new MatrixCursor(new String[]{"key", "value"});
                        for (Map.Entry<String, String> me : result.entrySet()) {
                            matrixCur.addRow(new Object[]{me.getKey(), me.getValue()});
                            Log.i(TAG, me.getKey() + " " + me.getValue());
                        }
                        cur = matrixCur;

                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                break;

            case "@":
                // retrieve all inserts from this port
                Log.v("query", selection);
                for (Map.Entry<String, String> me : map.entrySet()) {
                    cur.addRow(new String[]{me.getKey(), me.getValue()});
                }
                break;

            default:
                // Check which port has the query
                Log.v("query", selection);

                if (map.get(selection) != null) {
                    String value = map.get(selection);

                    String[] b = {selection, value};
                    cur.addRow(b);
                } else {
                    Log.i(TAG, "Forward to successorPort");
                    Message m = new Message();
                    m.setKey(selection);
                    m.setMsgType("ForwardSingleQueryRequest");
                    m.setSenderPort(myPort);
                    Log.i(TAG, "Value of origin port" + myPort);
                    m.setDestination(myPort);
                    m.setRemotePort(successorPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
                    // Wait till you receive the reply
                    while(true)
                    {
                        if(single_reply_flag)
                        {
                            single_reply_flag = false;
                            break;
                        }
                    }
                    Log.i(TAG, "Received reply.");
                    String value2 = result.get(selection);
                    String key = selection;
                    Log.i(TAG, key + " " + value2);
                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                    matrixCursor.addRow(new Object[]{key, value2});
                    cur = matrixCursor;
                }
        }
        return cur;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    //method for generating hash which is unique
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {
        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream objin= new ObjectInputStream(socket.getInputStream());
                    Message m = (Message) objin.readObject();
                    publishProgress(m);
                    objin.close();
                    socket.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        }

        protected void onProgressUpdate(Message... msgs) {
            Message m = msgs[0];
            String msgType = m.getMsgType();
            Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
            if (msgType.equals("JoinRequest")) {
                try {
                    String hash = genHash(Integer.toString(Integer.parseInt(m.getSenderPort()) / 2));
                    String port = Integer.toString(Integer.parseInt(m.getSenderPort()) / 2);
                    tMap.put(hash, port);
                    Log.i(TAG, "Node Joined: " + m.getSenderPort());
                    mapKeys = new String[tMap.size()];
                    int pos = 0;
                    for (String key : tMap.keySet()) {
                        mapKeys[pos++] = key;
                    }

                    // Send 3 messages.

                    // Tell senderPort it's predecessor and successor ports.
                    Message msg1 = new Message();
                    int index = Arrays.asList(mapKeys).indexOf(hash);

                    // If it's the last node
                    if (index == mapKeys.length - 1) {
                        msg1.setPreHash(mapKeys[index - 1]);
                        msg1.setSuccHash(mapKeys[0]);
                    }

                    // If it's first node
                    else if (index == 0) {
                        msg1.setPreHash(mapKeys[mapKeys.length - 1]);
                        msg1.setSuccHash(mapKeys[1]);
                    }
                    else {
                        msg1.setPreHash(mapKeys[index - 1]);
                        msg1.setSuccHash(mapKeys[index + 1]);
                    }
                    Log.i(TAG, "during joining getpre " + msg1.getPreHash());
                    Log.i(TAG, "during joining getsucc " + msg1.getSuccHash());

                    msg1.setRemotePort(m.getSenderPort());
                    msg1.setMsgType("JoinNode");

                    Log.i(TAG, "Remote Port for the node which has to be joined" + msg1.getRemotePort());

                    int indexPre = Arrays.asList(mapKeys).indexOf(msg1.getPreHash());
                    int indexSucc = Arrays.asList(mapKeys).indexOf(msg1.getSuccHash());

                    msg1.setPrePort(Integer.toString(Integer.parseInt(tMap.get(mapKeys[indexPre])) * 2));
                    msg1.setSuccPort(Integer.toString(Integer.parseInt(tMap.get(mapKeys[indexSucc])) * 2));

                    Message msg2 = new Message();
                    msg2.setSuccHash(hash);
                    msg2.setSuccPort(m.getSenderPort());
                    
                    for (Map.Entry<String, String> me : tMap.entrySet()) {
                        Log.i(TAG, " TreeMapvalues " + me.getKey() + " " + me.getValue());
                    }

                    Log.i(TAG, "Tree map values are " + tMap.get(mapKeys[indexPre]));
                    msg2.setRemotePort(Integer.toString(Integer.parseInt(tMap.get(mapKeys[indexPre])) * 2));
                    msg2.setMsgType("UpdatePredecessor");

                    Message msg3 = new Message();
                    msg3.setPreHash(hash);
                    msg3.setPrePort(m.getSenderPort());
                    msg3.setRemotePort(Integer.toString(Integer.parseInt(tMap.get(mapKeys[indexSucc])) * 2));
                    msg3.setMsgType("UpdateSuccessor");
                    Log.i(TAG, "Sending to msg1 to remoteport");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, null);
                    Log.i(TAG, "Sending to msg2 to remoteport");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, null);
                    Log.i(TAG, "Sending to msg3 to remoteport");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3, null);


                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
            if (msgType.equals("JoinNode")) {
                Log.i(TAG, "After Reaching JoinNode value of getPreHash " + m.getPreHash());
                Log.i(TAG, "After Reaching JoinNode value of getSuccHash " + m.getPreHash());
                predecessorHash = m.getPreHash();
                successorHash = m.getSuccHash();
                predecessorPort= m.getPrePort();
                successorPort= m.getSuccPort();
                Log.i(TAG, "Update: Predecessor = " + tMap.get(predecessorHash));
                Log.i(TAG, "Update: Successor = " + tMap.get(successorHash));
            }

            if (msgType.equals("UpdatePredecessor")) {
                successorHash = m.getSuccHash();
                successorPort= m.getSuccPort();
                Log.i(TAG, "Successor Updated= " + tMap.get(successorHash));
            }

            if (msgType.equals("UpdateSuccessor")) {
                predecessorHash = m.getPreHash();
                predecessorPort= m.getPrePort();
                Log.i(TAG, "Predecessor Updated= " + tMap.get(predecessorHash));
            }

            if (msgType.equals("ForwardInsertRequest")) {
                ContentValues cv = new ContentValues();
                cv.put("key", m.getKey());
                cv.put("value", m.getValue());
                insert(providerUri, cv);
            }
            if (msgType.equals("ForwardSingleQueryRequest")) {
                Log.i(TAG, "received get a single query request");
                String key0 = m.getKey();
                if (map.get(key0) != null) {
                    String value = map.get(key0);
                    Message msg = new Message();
                    msg.setMsgType("Response");
                    HashMap<String, String> result = new HashMap<>();
                    result.put(key0, value);
                    msg.setResult(result);
                    msg.setRemotePort(m.getDestination());
                    msg.setSenderPort(myPort);
                    Log.i(TAG, "Found Query Sending Back to destination port " + m.getRemotePort());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, null);

                }
                else {
                    // forward it to your successor
                    Log.i(TAG, "Didn't find the result again.. Forwarding to successor" + m.getKey());
                    m.setRemotePort(successorPort);
                    //Log.i(TAG, "value of destination port is " + m.getDestination());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
                }
            }
            //Response send to origin port
            if (msgType.equals("Response")) {
                Log.i(TAG, "Reponse received for single query");
                result = m.getResult();
                //exiting wait
                single_reply_flag= true;
            }
            //Responses received at origin port
            if (msgType.equals("ResponseAll")) {
                Log.i(TAG, "Response received for queries received from all the ports");
                result = m.getResult();
                //exiting wait
                all_reply_flag= true;
            }
            if (msgType.equals("ForwardGetQueriesRequest")) {

                HashMap<String, String> result2;
                result2 = m.getResult();

                if (m.getRemotePort().equals(m.getSenderPort())) {

                    for (Map.Entry<String, String> me : map.entrySet()) {
                        result2.put(me.getKey(), me.getValue());
                    }
                    m.setMsgType("ResponseAll");
                    m.setResult(result2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
                } else {
                    // Add your own queries and forward to successor port
                    for (Map.Entry<String, String> me : map.entrySet()) {
                        Log.i(TAG, " Entryvalues " + me.getKey() + " " + me.getValue());
                        result2.put(me.getKey(), me.getValue());
                    }
                    m.setResult(result2);
                    m.setRemotePort(successorPort);
                    Log.i(TAG, "Forwarding to the successorport " + m.getRemotePort());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);

                }
            }
        }
    }
    private class ClientTask extends AsyncTask<Message, Void, Void> {
        @Override
        protected Void doInBackground(Message... msgs) {
            Message msg = msgs[0];
            Socket socket;
            {
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg.getRemotePort()));
                    ObjectOutputStream objout= new ObjectOutputStream(socket.getOutputStream());
                    objout.writeObject(msg);
                    objout.flush();
                    objout.close();
                    socket.close();

                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }
}