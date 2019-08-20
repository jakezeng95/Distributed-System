package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.inputmethodservice.KeyboardView;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.acl.Group;

import android.view.View.OnClickListener;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());



        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        //code from PA1
        TelephonyManager tel = (TelephonyManager)  this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (IOException e){
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final EditText editText1 = (EditText) findViewById(R.id.editText1);
        findViewById(R.id.button4).setOnClickListener(
                new OnClickListener(){
                @Override
                public void onClick(View v){
                    String msg = editText1.getText().toString();
                    editText1.setText("");
                    //tv.append(msg + "\n");

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                    return;
                }

                }
        );





    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{
        private int count = 0;
        private ContentResolver cr = getContentResolver();
        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
        @Override
        protected Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];

            while (true){
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream in = new DataInputStream(
                            new BufferedInputStream(socket.getInputStream())
                    );
                    String line = in.readUTF();

                    publishProgress(line);
                    in.close();
                    socket.close();
                }catch (IOException e){
                    publishProgress(e.toString());
                    System.out.print(e);
                }

            }
        }
        protected void onProgressUpdate(String... strings){
            String strReceived = strings[0].trim();
            TextView textView1 = (TextView) findViewById(R.id.textView1);
            textView1.append(strReceived + "\t\n");

            FileOutputStream outputStream;
            try {

                ContentValues cv = new ContentValues();
                String fileName = Integer.toString(count);
                cv.put("key", Integer.toString(count));
                count++;
                cv.put("value", strReceived);
                cr.insert(uri, cv);
                outputStream = openFileOutput(fileName, Context.MODE_PRIVATE);
                outputStream.write(strReceived.getBytes());
                outputStream.close();


            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }

            return;

        }
        //method from OnPTestClickListener.java
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
            for (String remote_port: REMOTE_PORTS) {
                try {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));
                    String msgToSend = msgs[0];
                    DataOutput out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF(msgToSend);
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e){
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            return null;
        }
    }
}
