package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class    SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static final String FIRST_NODE = "11108";
    static TreeMap<String, String> exisitNodes = new TreeMap<String, String>();
    static Map<String, String> mapping = new HashMap<String, String>();
    static String predecessor = "";
    static String sucessor = "";
    static String port = "";
    static Set<String> insertedFiles = new HashSet<String>();



    public void deleteLocally(){
        for (String file: insertedFiles) {
            getContext().deleteFile(file);
        }
    }
    public void deleteLocalFile(String filename){
        getContext().deleteFile(filename);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if (selection.equals("*")){
            deleteLocally();
            try {
                Socket deleteToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sucessor));
                DataOutputStream out = new DataOutputStream(deleteToSuc.getOutputStream());
                String query = "detele," + selection + "," + port;
                out.writeUTF(query);
                Log.i("delete", "pass " + selection + " to successor");

                out.close();
                deleteToSuc.close();

                // use this string to build a cursor;

            }catch (IOException e){
                Log.e("query","can't create socket");
            }
        }else if (selection.equals("@")){
            deleteLocally();
        }else if (insertedFiles.contains(selection)){
            deleteLocalFile(selection);
        }else {
            try {
                Socket deleteToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sucessor));
                DataOutputStream out = new DataOutputStream(deleteToSuc.getOutputStream());
                String query = "detele," + selection + "," + port;
                out.writeUTF(query);
                Log.i("delete", "pass " + selection + " to successor");

                out.close();
                deleteToSuc.close();

                // use this string to build a cursor;

            }catch (IOException e){
                Log.e("query","can't create socket");
            }
        }
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
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        String fileName = key;


        try {
            String hashedFile = genHash(key);
            String hashedNode = mapping.get(port);
            String hashedPre = mapping.get(predecessor);
            boolean normalcase = (hashedNode.compareTo(hashedPre) > 0) && (hashedFile.compareTo(hashedPre) > 0)
                    && (hashedFile.compareTo(hashedNode) <=0);
            boolean specialcase1 = (hashedNode.compareTo(hashedPre) < 0) && (hashedFile.compareTo(hashedPre) > 0);
            boolean specialcase2 = (hashedNode.compareTo(hashedPre) < 0) && (hashedFile.compareTo(hashedNode) < 0);
            boolean oneport = hashedNode.equals(hashedPre);

            if ( normalcase || specialcase1 || specialcase2 || oneport){
                FileOutputStream fos;
                fos = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                fos.write(value.getBytes());
                fos.close();
                insertedFiles.add(fileName);
                Log.i("insert", values.toString() + "success");

            }
            else {
                Socket passToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sucessor));
                DataOutput out = new DataOutputStream(passToSuc.getOutputStream());
                String cv = "insert,key," + fileName + ",value," + value ;
                out.writeUTF(cv);
                Log.i("insert","pass " + cv + " to successor");

                DataInputStream in = new DataInputStream(
                        new BufferedInputStream(passToSuc.getInputStream())
                );
                String ack = in.readUTF();
                if (ack.contains("insertAck"))
                    Log.i("insert", "ack received");
                else
                    Log.i("insert", "check ack");

                ((DataOutputStream) out).close();
                in.close();
                passToSuc.close();

            }

        }catch (EOFException e){
            Log.e("insert", "Can't read pass response");
        }
        catch (Exception e){
            Log.e("insert","Can't write to file");
            e.printStackTrace();
        }
        return uri;


    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        port = myPort;
        Log.i("portnumber",myPort);

        for (String remote_port: REMOTE_PORTS){
            try{
                int portNum =Integer.valueOf(remote_port)/2;
                String result = genHash(Integer.toString(portNum));
                mapping.put(remote_port, result);
                mapping.put(result, remote_port);
            }catch (NoSuchAlgorithmException e){
                Log.e(TAG, "Can't hash");
            }
        }

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (IOException e){
            Log.e(TAG, "Can't create a Server");
            System.out.println("Can't create a Server");
        }

        if (!myPort.equals(FIRST_NODE)){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, FIRST_NODE, myPort);
        }else {
            exisitNodes.put(mapping.get(FIRST_NODE), FIRST_NODE);
            sucessor = FIRST_NODE;
            predecessor = FIRST_NODE;
        }
        return false;
    }


    public Cursor buildCursor(MatrixCursor cursor, String filename){
        try {
            FileInputStream fis = getContext().openFileInput(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            StringBuilder sb = new StringBuilder();
            String temp;
            while ((temp = br.readLine()) != null){
                sb.append(temp);
            }

            String value = sb.toString();
            cursor.addRow(new String[]{filename, value});
        }catch (Exception e){
            Log.e("query", "can't open file");
        }
        return cursor;
    }
    public Cursor buildCursor(String[] keyAndValue){
        String[] columns = new String[]{"key","value"};
        MatrixCursor re = new MatrixCursor(columns);
        re.addRow(keyAndValue);
        return re;
    }
    public Cursor buildCursor(MatrixCursor cursor, String[] keyAndValue){

        cursor.addRow(keyAndValue);
        return cursor;
    }
   @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
       String[] columns = new String[]{"key","value"};
       MatrixCursor re = new MatrixCursor(columns);
        // TODO Auto-generated method stub
        if (selection.equals("*")){
            try {
                Socket queryToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sucessor));
                DataOutputStream out = new DataOutputStream(queryToSuc.getOutputStream());
                String query = "query," + selection + "," + port;
                out.writeUTF(query);
                Log.i("query", "pass query" + selection + " to successor");

                DataInputStream in = new DataInputStream(new BufferedInputStream(queryToSuc.getInputStream()));
                String localPairs = readLocalFiles(insertedFiles);
                String keyValuePairs = in.readUTF();

                in.close();
                out.close();
                queryToSuc.close();
                String allPairs = localPairs + keyValuePairs;
                String[] pairs = allPairs.split("\n");
                for (String pair: pairs){
                    buildCursor(re, pair.split(","));
                }
                return re;

                // use this string to build a cursor;

            }catch (IOException e){
                Log.e("query","can't create socket");
            }

        }else if (selection.equals("@")){
            for (String file: insertedFiles){
                buildCursor(re, file);
            }
            return re;
        }
        else if (insertedFiles.contains(selection)){
            Log.i("query","cursor is build" + selection);
            return buildCursor(re, selection);
        }else{
            try {
                Socket queryToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sucessor));
                DataOutputStream out = new DataOutputStream(queryToSuc.getOutputStream());
                String query = "query," + selection;
                out.writeUTF(query);
                Log.i("query", "pass query" + selection + " to successor");

                DataInputStream in = new DataInputStream(new BufferedInputStream(queryToSuc.getInputStream()));
                String keyValuePair = in.readUTF();
                in.close();
                out.close();
                queryToSuc.close();
                String[] list = keyValuePair.split(",");
                Log.i("query", "cursor is built" + keyValuePair);
                return buildCursor(list);

                // use this string to build a cursor;

            }catch (IOException e){
                Log.e("query","can't create socket");
            }
        }
        return null;

    }
    public String readLocalFiles(Set<String> set) {
        StringBuilder sb = new StringBuilder();
        try {
            for (String file: set) {
                FileInputStream fis = getContext().openFileInput(file);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                StringBuilder tempbuilder = new StringBuilder();
                String temp;
                while ((temp = br.readLine()) != null) {
                    tempbuilder.append(temp);
                }
                String value = tempbuilder.toString();
                sb.append(file + "," + value + "\n");

            }
        } catch (IOException e) {
            Log.e("readLocalFiles", "Can't read local files");
        }
        return sb.toString();
    }
    public String findPair(String filename) {
        StringBuilder sb = new StringBuilder();
        try {
                FileInputStream fis = getContext().openFileInput(filename);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));

                String temp;
                while ((temp = br.readLine()) != null) {
                    sb.append(temp);
                }
                String value = sb.toString();
                String re = filename + "," + value;
                return re;
        } catch (IOException e) {
            Log.e("readLocalFiles", "Can't read local files");
        }
        return null;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {

        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while (true) {
                try {

                    Socket socket = serverSocket.accept();
                    Log.i("server","socket accept");
                    DataInputStream in = new DataInputStream(
                            new BufferedInputStream(socket.getInputStream())
                    );
                    DataOutputStream out = new DataOutputStream(
                            new BufferedOutputStream(socket.getOutputStream())
                    );
                    Log.i("server", "socket received");


                    String line = in.readUTF();
                    String[] strs = line.split(",");

                    if (strs[0].equals("join")) {
                        exisitNodes.put(mapping.get(strs[1]), strs[1]);
                        String pre = mapping.get(exisitNodes.lowerKey(mapping.get(strs[1])));
                        if (pre == null)
                            pre = mapping.get(exisitNodes.lastKey());
                        String suc = mapping.get(exisitNodes.higherKey(mapping.get(strs[1])));
                        if(suc == null)
                            suc = mapping.get(exisitNodes.firstKey());
                        String output = "pre,"  + pre + ",suc," + suc;
                        out.writeUTF(output);
                        Log.i("server", "response sent");
                        System.out.println("response sent");

                        out.close();
                        in.close();
                        socket.close();

                        Socket s_pre = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(pre));
                        DataOutputStream out_pre = new DataOutputStream(s_pre.getOutputStream());
                        String affect_pre = "affect,suc," + strs[1];
                        out_pre.writeUTF(affect_pre);
                        out_pre.close();
                        s_pre.close();

                        Socket s_suc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(suc));
                        DataOutputStream out_suc = new DataOutputStream(s_suc.getOutputStream());
                        String affect_suc = "affect,pre," + strs[1];
                        out_suc.writeUTF(affect_suc);
                        out_suc.close();
                        s_suc.close();
                    }
                    else if (strs[0].equals("affect")){
                        if (strs[1].equals("suc")) {
                            sucessor = strs[2];
                            Log.i("update", "sucessor" + strs[2]);
                        }
                        else {
                            predecessor = strs[2];
                            Log.i("update", "predecessor" + strs[2]);

                        }

                        out.close();
                        in.close();
                        socket.close();
                    }
                    else if (strs[0].equals("insert")){

                        Log.i("insert", "pass received");
                        ContentValues cv = new ContentValues();
                        cv.put("key", strs[2]);
                        cv.put("value", strs[4]);
                        insert(mUri, cv);
                        out.writeUTF("insertAck");

                        out.close();
                        in.close();
                        socket.close();

                    }
                    else if (strs[0].equals("query")){
                        if (strs[1].equals("*")){
                            String localResult = readLocalFiles(insertedFiles);
                            if (sucessor.equals(strs[2])){
                                out.writeUTF(localResult);

                                out.close();
                                in.close();
                                socket.close();
                            }else {
                                Socket suc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(sucessor));
                                DataOutputStream outToSuc = new DataOutputStream(suc.getOutputStream());
                                outToSuc.writeUTF(line);
                                DataInputStream inFromSuc = new DataInputStream(new BufferedInputStream(suc.getInputStream()));
                                String sucResult = inFromSuc.readUTF();
                                String combineResult = localResult + sucResult;

                                outToSuc.close();
                                inFromSuc.close();
                                suc.close();

                                out.writeUTF(combineResult);
                                out.close();
                                in.close();
                                socket.close();
                            }

                        }else {
                            if (insertedFiles.contains(strs[1])){
                                String result = findPair(strs[1]);
                                out.writeUTF(result);
                                out.close();
                                in.close();
                                socket.close();

                            }else {
                                Socket suc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(sucessor));
                                DataOutputStream outToSuc = new DataOutputStream(suc.getOutputStream());
                                outToSuc.writeUTF(line);
                                DataInputStream inFromSuc = new DataInputStream(new BufferedInputStream(suc.getInputStream()));
                                String sucResult = inFromSuc.readUTF();
                                outToSuc.close();
                                inFromSuc.close();
                                suc.close();

                                out.writeUTF(sucResult);
                                out.close();
                                in.close();
                                socket.close();

                            }
                        }
                    }else if (strs[0].equals("delete")){
                        if (strs[1].equals("*")){
                            deleteLocally();
                            if (!sucessor.equals(strs[2])){
                                Socket deleteToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(sucessor));
                                DataOutputStream outToSuc = new DataOutputStream(deleteToSuc.getOutputStream());
                                outToSuc.writeUTF(line);
                                Log.i("delete", "pass * to successor");

                                outToSuc.close();
                                deleteToSuc.close();
                            }
                        }else if (insertedFiles.contains(strs[1])){
                            deleteLocalFile(strs[1]);
                        }else {
                            try {
                                Socket deleteToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(sucessor));
                                DataOutputStream outToSuc = new DataOutputStream(deleteToSuc.getOutputStream());
                                outToSuc.writeUTF(line);
                                Log.i("delete", "pass " + strs[1] + " to successor");
                                outToSuc.close();
                                deleteToSuc.close();

                                // use this string to build a cursor;

                            }catch (IOException e){
                                Log.e("query","can't create socket");
                            }
                        }
                    }
                } catch (IOException e) {
                    Log.e(TAG, "Can't create ServerSocket");
                }

            }
        }
        @Override
        protected void onProgressUpdate(String... values){

        }
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

    }
    private class ClientTask extends AsyncTask<String, Void, Void>{
        @Override
        protected Void doInBackground(String... msgs){
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[0]));
                Log.i("client", "socket created");

                String msgToSend = "join" + "," + msgs[1];
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF(msgToSend);
                Log.i("client", "join sent");
                DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

                String recv = in.readUTF();
                String[] recvList = recv.split(",");
                predecessor = recvList[1];
                sucessor = recvList[3];
                Log.i("client", "predecessor=" + predecessor);
                Log.i("client", "sucessor=" + sucessor);

                in.close();
                out.close();
                socket.close();
            }catch (IOException e){
                predecessor = msgs[1];
                sucessor = msgs[1];
            }
            return null;
        }
    }
}

