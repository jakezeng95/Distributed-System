package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
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

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
	static final int SERVER_PORT = 10000;
	static TreeMap<String, String> DynamoRing = new TreeMap<String, String>();
	static Map<String, String> mapping = new HashMap<String, String>();
    static String port;
	static String predecessor1 = "";
	static String predecessor2 = "";
	static String sucessor1 = "";
	static String sucessor2 = "";
	static Set<String> insertedFiles = new HashSet<String>();
	static Map<String, String> predecessor1Files = new HashMap<String, String>();
    static Map<String, String> predecessor2Files = new HashMap<String, String>();
    static Map<String, String> currentFiles = new HashMap<String, String>();
    static String[] staleFiles;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        if (selection.equals("*")){
            deleteLocally();
            try {
                Socket deleteToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sucessor1));
                DataOutputStream out = new DataOutputStream(deleteToSuc.getOutputStream());
                String query = "delete," + selection + "," + port;
                out.writeUTF(query);
                Log.i("delete", "pass " + selection + " to successor");

                out.close();
                deleteToSuc.close();

                // use this string to build a cursor;

            }catch (IOException e){
                Log.e("delete","can't create socket");
            }
        }else if (selection.equals("@")){
            deleteLocally();
        }else {
            try {
                String coordinator = findDestination(selection);
                Socket deleteToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(coordinator));
                DataOutputStream out = new DataOutputStream(deleteToSuc.getOutputStream());
                String query = "delete," + selection;
                out.writeUTF(query);
                Log.i("delete", "pass " + selection + " to destination");

                out.close();
                deleteToSuc.close();

                // use this string to build a cursor;

            }catch (IOException e){
                Log.e("delete","destination already failed");
                try{
                    String coordinator = findReplica1(selection);
                    Socket deleteToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(coordinator));
                    DataOutputStream out = new DataOutputStream(deleteToSuc.getOutputStream());
                    String query = "coorfail_delete," + selection;
                    out.writeUTF(query);
                    Log.i("delete", "pass " + selection + " to destination's replica");
                    out.close();
                    deleteToSuc.close();
                }catch (IOException innerE){
                    Log.i("delete", "can't pass " + selection + " to destination's replica");

                }

            }
        }
        return 0;

	}
    public void deleteLocally(){
        for (String file: insertedFiles) {
            getContext().deleteFile(file);
        }
    }
    public void deleteLocalFile(String filename){
        getContext().deleteFile(filename);
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}
    public String findDestination(String filename){
        try{
            String hashedFileName = genHash(filename);
            for (Map.Entry<String, String> entry: DynamoRing.entrySet()){
                String HashNode = entry.getKey();
                Map.Entry<String, String> nextEntry = DynamoRing.higherEntry(HashNode);
                String coordinater = nextEntry == null? DynamoRing.firstEntry().getValue(): nextEntry.getValue();
                String HashCoordinater = mapping.get(coordinater);

                boolean normalcase = (HashCoordinater.compareTo(HashNode) > 0) && (hashedFileName.compareTo(HashNode) > 0)
                        && (hashedFileName.compareTo(HashCoordinater) <=0);
                boolean specialcase1 = (HashCoordinater.compareTo(HashNode) < 0) && (hashedFileName.compareTo(HashNode) > 0);
                boolean specialcase2 = (HashCoordinater.compareTo(HashNode) < 0) && (hashedFileName.compareTo(HashCoordinater) < 0);
                boolean oneport = HashCoordinater.equals(HashNode);

                if( normalcase || specialcase1 || specialcase2 || oneport){
                   return coordinater;
                }
            }
        }catch (NoSuchAlgorithmException e){
            Log.e("findDestination", "can't hash");
        }
        return null;
    }
    public String findReplica1(String filename){
	    String coordinate = findDestination(filename);
	    String replica1 = DynamoRing.higherKey(mapping.get(coordinate)) == null? DynamoRing.firstEntry().getValue():
                DynamoRing.higherEntry(mapping.get(coordinate)).getValue();
	    return replica1;
    }
	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        String fileName = key;

        try{
            String coordinater = findDestination(fileName);
            Socket passToDestination = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(coordinater));
            DataOutputStream out = new DataOutputStream(passToDestination.getOutputStream());


            String cv = "insert,key," + fileName + ",value," + value ;
            out.writeUTF(cv);
            Log.i("insert","pass " + cv + " to destination " + coordinater);

            DataInputStream in = new DataInputStream(new BufferedInputStream(passToDestination.getInputStream()));
            String response = in.readUTF();
            Log.i("insert", response);


            out.close();
            in.close();
            passToDestination.close();

        }catch (UnknownHostException e){
            Log.e("insert", "can't create socket");
        }catch (IOException e){
            Log.e("insert", "No response, inform replica1");
            String replica1 = findReplica1(fileName);
            try {
                Socket passToRep1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(replica1));
            DataOutputStream out = new DataOutputStream(passToRep1.getOutputStream());
            String cv = "coor_fail,key," + fileName + ",value," + value ;
            out.writeUTF(cv);
            Log.i("coor_fail","pass " + cv + " to destination " + replica1);

            DataInputStream in = new DataInputStream(new BufferedInputStream(passToRep1.getInputStream()));
            String response = in.readUTF();
            Log.i("coor_fail", response);

            out.close();
            in.close();
            passToRep1.close();
            }catch (IOException innerE){
                Log.e("coor_fail", "replica1 also failed");
            }

        }

		return null;
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
				DynamoRing.put(result, remote_port);
				mapping.put(remote_port, result);
			}catch (NoSuchAlgorithmException e){
				Log.e(TAG, "Can't hash");
			}
		}
        sucessor1 = DynamoRing.higherKey(mapping.get(myPort)) == null? DynamoRing.firstEntry().getValue(): DynamoRing.higherEntry(mapping.get(myPort)).getValue();
		sucessor2 = DynamoRing.higherKey(mapping.get(sucessor1)) == null? DynamoRing.firstEntry().getValue(): DynamoRing.higherEntry(mapping.get(sucessor1)).getValue();
		predecessor1 = DynamoRing.lowerKey(mapping.get(myPort)) == null? DynamoRing.lastEntry().getValue(): DynamoRing.lowerEntry(mapping.get(myPort)).getValue();
		predecessor2 = DynamoRing.lowerKey(mapping.get(predecessor1)) == null? DynamoRing.lastEntry().getValue(): DynamoRing.lowerEntry(mapping.get(predecessor1)).getValue();


        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (IOException e){
            Log.e(TAG, "Can't create a Server");
            System.out.println("Can't create a Server");
        }

        staleFiles = getContext().fileList();
        for (String staleFile: staleFiles){
            deleteLocalFile(staleFile);
        }
        Log.i("oncreate","delete all stalefiles");

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, null, myPort);






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
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        String[] columns = new String[]{"key","value"};
        MatrixCursor re = new MatrixCursor(columns);
        // TODO Auto-generated method stub
        if (selection.equals("*")){
            try {
                Socket queryToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sucessor1));
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
                Log.e("query","can't pass * to fail node");
                try{
                    Socket queryToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(sucessor2));
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
                }catch (IOException Innere){
                    Log.e("query","can't pass * to fail node's successor");
                }

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
            try{
                    String coordinater = findDestination(selection);
                    Socket passToDestination = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(coordinater));
                    DataOutputStream out = new DataOutputStream(passToDestination.getOutputStream());
                    String query = "query," + selection;
                    out.writeUTF(query);
                    Log.i("query","pass query" + selection + " to destination " + coordinater);
                    DataInputStream in = new DataInputStream(new BufferedInputStream(passToDestination.getInputStream()));
                    String keyValuePair = in.readUTF();
                    in.close();
                    out.close();
                    passToDestination.close();
                    String[] list = keyValuePair.split(",");
                    Log.i("query", "cursor is built" + keyValuePair);
                    return buildCursor(list);

            }catch (UnknownHostException e){
                Log.e("insert", "can't create socket");
            }catch (IOException e){
                Log.e("insert", "can't connect query destination");
                try{
                    String coordinater = findReplica1(selection);
                    Socket passToDestination = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(coordinater));
                    DataOutputStream out = new DataOutputStream(passToDestination.getOutputStream());
                    String query = "query," + selection;
                    out.writeUTF(query);
                    Log.i("query","pass query" + selection + " to destination " + coordinater);
                    DataInputStream in = new DataInputStream(new BufferedInputStream(passToDestination.getInputStream()));
                    String keyValuePair = in.readUTF();
                    in.close();
                    out.close();
                    passToDestination.close();
                    String[] list = keyValuePair.split(",");
                    Log.i("query", "cursor is built" + keyValuePair);
                    return buildCursor(list);
                }catch (IOException innerE){
                    Log.e("query", "can't connect query destination's successor");
                }
            }
            return null;
        }
        return null;
		// TODO Auto-generated method stub

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	public String mapToKeyValues(Map<String, String> map){
	    StringBuilder sb = new StringBuilder();
	    for (Map.Entry<String, String> entry: map.entrySet()){
	        String eachFile = entry.getKey() + "/" + entry.getValue();
	        sb.append(eachFile+",");
        }
        return sb.toString();

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

                    String line = in.readUTF();
                    String[] strs = line.split(",");

                    if (strs[0].equals("insert")) {


                        Log.i("insert", "insert request received");
                        FileOutputStream fos;
                        fos = getContext().openFileOutput(strs[2], Context.MODE_PRIVATE);
                        fos.write(strs[4].getBytes());
                        fos.close();
                        currentFiles.put(strs[2], strs[4]);
                        insertedFiles.add(strs[2]);
                        Log.i("insert",  strs[2] +"," + strs[4] +","+ "success");
                        out.writeUTF("ack");
                        out.close();
                        in.close();
                        socket.close();

                        String storeToReplica1 = "replica1,key," + strs[2] + ",value," + strs[4] ;
                        Socket ToRep1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(sucessor1));
                        DataOutputStream outToRep1 = new DataOutputStream(ToRep1.getOutputStream());
                        outToRep1.writeUTF(storeToReplica1);
                        Log.i("replica","send to replica node1");
                        outToRep1.close();
                        ToRep1.close();

                        String storeToReplica2 = "replica2,key," + strs[2] + ",value," + strs[4];
                        Socket ToRep2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(sucessor2));
                        DataOutputStream outToRep2 = new DataOutputStream(ToRep2.getOutputStream());
                        outToRep2.writeUTF(storeToReplica2);
                        Log.i("replica","send to replica node2");
                        outToRep2.close();
                        ToRep2.close();


                    }else if (strs[0].equals("coor_fail")){

                        Log.i("coor_fail", "replica request received");
                        FileOutputStream fos;
                        fos = getContext().openFileOutput(strs[2], Context.MODE_PRIVATE);
                        fos.write(strs[4].getBytes());
                        fos.close();
                        insertedFiles.add(strs[2]);
                        predecessor1Files.put(strs[2], strs[4]);
                        Log.i("insert",  strs[2] +"," + strs[4] +","+ "success");
                        out.writeUTF("replica ack");
                        out.close();
                        in.close();
                        socket.close();

                        String storeToReplica = "replica2,key," + strs[2] + ",value," + strs[4] ;
                        Socket ToRep1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(sucessor1));
                        DataOutputStream outToRep1 = new DataOutputStream(ToRep1.getOutputStream());
                        outToRep1.writeUTF(storeToReplica);
                        Log.i("coor_fail","send to replica node2");
                        outToRep1.close();
                        ToRep1.close();

                    }
                    else if (strs[0].contains("replica")){
                        Log.i("replica", "replica request received");
                        FileOutputStream fos;
                        fos = getContext().openFileOutput(strs[2], Context.MODE_PRIVATE);
                        fos.write(strs[4].getBytes());
                        fos.close();
                        insertedFiles.add(strs[2]);
                        if (strs[0].equals("replica1")){
                            predecessor1Files.put(strs[2], strs[4]);
                        }else if (strs[0].equals("replica2")){
                            predecessor2Files.put(strs[2], strs[4]);
                        }
                        Log.i("replicate",  strs[2] +"," + strs[4] +","+ "success");
                        in.close();
                        out.close();
                        socket.close();
                    }
                    else if (strs[0].equals("query")){
                        if (strs[1].equals("*")){
                            String localResult = readLocalFiles(insertedFiles);
                            if (sucessor1.equals(strs[2])){
                                out.writeUTF(localResult);

                                out.close();
                                in.close();
                                socket.close();
                            }else {
                                try {
                                    Socket suc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(sucessor1));

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
                                }catch (IOException e){
                                    Log.e("query","can't pass * to fail node");
                                    if (sucessor2.equals(strs[2])){
                                        out.writeUTF(localResult);

                                        out.close();
                                        in.close();
                                        socket.close();
                                    }else {
                                        try {
                                            Socket suc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                    Integer.parseInt(sucessor2));
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
                                        }catch (IOException InnerE){
                                            Log.i("query","can't pass * to fail node's successor");
                                        }
                                    }
                                }

                            }

                        }else {
                            if (insertedFiles.contains(strs[1])){
                                String result = findPair(strs[1]);
                                out.writeUTF(result);
                                out.close();
                                in.close();
                                socket.close();

                            }else {
                                Log.i("query","did not find the right coordinater");

                            }
                        }
                    }else if (strs[0].equals("delete")) {
                        if (strs[1].equals("*")) {
                            deleteLocally();
                            if (!sucessor1.equals(strs[2])) {
                                try {
                                    out.close();
                                    in.close();
                                    socket.close();

                                    Socket deleteToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            Integer.parseInt(sucessor1));
                                    DataOutputStream outToSuc = new DataOutputStream(deleteToSuc.getOutputStream());
                                    outToSuc.writeUTF(line);
                                    Log.i("delete", "pass * to successor");

                                    outToSuc.close();
                                    deleteToSuc.close();
                                } catch (IOException e) {
                                    if (!sucessor2.equals(strs[2])) {
                                        try {
                                            out.close();
                                            in.close();
                                            socket.close();

                                            Socket deleteToSuc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                    Integer.parseInt(sucessor2));
                                            DataOutputStream outToSuc = new DataOutputStream(deleteToSuc.getOutputStream());
                                            outToSuc.writeUTF(line);
                                            Log.i("delete", "pass * to fail node's successor");

                                            outToSuc.close();
                                            deleteToSuc.close();
                                        } catch (IOException innerE) {
                                            Log.e("delete", "can't pass * to fail node's successor");
                                        }
                                    }
                                }
                            }
                        } else {
                            deleteLocalFile(strs[1]);
                            currentFiles.remove(strs[1]);
                            Socket deleteToReplica1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(sucessor1));
                            DataOutputStream outToReplica1 = new DataOutputStream(deleteToReplica1.getOutputStream());
                            String inform1 = "delete rep," + strs[1] + ",pre1";
                            outToReplica1.writeUTF(inform1);
                            Log.i("delete", "delete " + strs[1] + " in successor1");

                            outToReplica1.close();
                            deleteToReplica1.close();

                            Socket deleteToReplica2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(sucessor2));
                            DataOutputStream outToReplica2 = new DataOutputStream(deleteToReplica2.getOutputStream());
                            String inform2 = "delete rep," + strs[1] + ",pre2";
                            outToReplica2.writeUTF(inform2);
                            Log.i("delete", "delete " + strs[1] + " in successor2");

                            outToReplica2.close();
                            deleteToReplica2.close();
                            out.close();
                            in.close();
                            socket.close();
                        }
                    }else if(strs[0].equals("coorfail_delete")) {
                        predecessor1Files.remove(strs[1]);
                        deleteLocalFile(strs[1]);
                        Socket deleteToReplica1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(sucessor1));
                        DataOutputStream outToReplica1 = new DataOutputStream(deleteToReplica1.getOutputStream());
                        String inform1 = "delete rep," + strs[1] + ",pre2";
                        outToReplica1.writeUTF(inform1);
                        Log.i("delete", "delete " + strs[1] + " in successor1");

                        outToReplica1.close();
                        deleteToReplica1.close();
                        out.close();
                        in.close();
                        socket.close();

                    }else if (strs[0].equals("delete rep")){
                        deleteLocalFile(strs[1]);
                        Log.i("delete", "replica " + strs[1] + " deleted");
                        if (strs[2].equals("pre1")) predecessor1Files.remove(strs[1]);
                        else if (strs[2].equals("pre2")) predecessor2Files.remove(strs[1]);

                        out.close();
                        in.close();
                        socket.close();
                    }else if (strs[0].equals("recover")){
                        if (strs[1].equals("pre1")){
                            StringBuilder sb = new StringBuilder();
                            if (predecessor1Files.isEmpty()){
                                Log.i("recover","there's nothing to sychronize");
                            }else {
                                String pre1files = mapToKeyValues(predecessor1Files);
                                sb.append("pre2," + pre1files + ";");
                            }
                            if (currentFiles.isEmpty()){
                                Log.i("recover","there's nothing to sychronize");
                            }else {
                                String currentfiles = mapToKeyValues(currentFiles);
                                sb.append("pre1," + currentfiles + ";");
                            }
                            String res = sb.toString();
                            if (res == null) out.writeUTF("nothing");
                            else out.writeUTF(res);
                            out.close();
                            in.close();
                            socket.close();

                        }else {

                            if(predecessor1Files.isEmpty()){
                                Log.i("recover","there's nothing to sychronize");
                                out.writeUTF("nothing");
                            }else {
                                Log.i("recover","send K&V to recover node");
                                String res = "cur," + mapToKeyValues(predecessor1Files);
                                out.writeUTF(res);
                            }
                            out.close();
                            in.close();
                            socket.close();
                        }

                    }
                }catch (IOException e){
                    Log.e("socket","IO exception");
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
                Socket toPredecessor1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(predecessor1));
                Log.i("client", "recover from pre1 socket created");

                String msgToPre1 = "recover" + ",pre1," + msgs[1];
                DataOutputStream outToPre1 = new DataOutputStream(toPredecessor1.getOutputStream());
                outToPre1.writeUTF(msgToPre1);
                Log.i("client", "recover sent");
                DataInputStream recvFromPre = new DataInputStream(new BufferedInputStream(toPredecessor1.getInputStream()));

                String res1 = recvFromPre.readUTF();
                recvFromPre.close();
                outToPre1.close();
                toPredecessor1.close();
                synchronize(res1);

                Socket toSuccessor1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(sucessor1));
                Log.i("client", "recover from suc1 socket created");
                String msgToSuc1 = "recover" + ",suc1," + msgs[1];
                DataOutputStream outToSuc1 = new DataOutputStream(toSuccessor1.getOutputStream());
                outToSuc1.writeUTF(msgToSuc1);
                Log.i("client", "recover sent");
                DataInputStream recvFromSuc = new DataInputStream(new BufferedInputStream(toSuccessor1.getInputStream()));
                String res2 = recvFromSuc.readUTF();

                recvFromSuc.close();
                outToSuc1.close();
                toPredecessor1.close();
                synchronize(res2);


            }catch (IOException e){

            }
            return null;
        }
    }
    public void synchronize(String s){
	    if (!s.equals("nothing")){
	        String[] seperateMap =  s.split(";");
	        for (String map: seperateMap){
	           String[] seperatePairs =  map.split(",");

	               for (int i = 1; i < seperatePairs.length; i++) {
                       String[] keyValue = seperatePairs[i].split("/");
                       if (seperatePairs[0].equals("pre2")) predecessor2Files.put(keyValue[0], keyValue[1]);
                       else if (seperatePairs[0].equals("pre1")) predecessor1Files.put(keyValue[0], keyValue[1]);
                       else if (seperatePairs[0].equals("cur")) currentFiles.put(keyValue[0], keyValue[1]);
                       try {
                           FileOutputStream fos;
                           fos = getContext().openFileOutput(keyValue[0], Context.MODE_PRIVATE);
                           fos.write(keyValue[1].getBytes());
                           fos.close();
                           insertedFiles.add(keyValue[0]);
                       } catch (IOException e) {
                           Log.e("recover", "can't write");
                       }
                   }


            }
        }
    }
}
