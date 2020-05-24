package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final String[] ports = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
	static final String[] plist = {"5554", "5556", "5558", "5560", "5562"};
	static final int SERVER_PORT = 10000;
	Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo");
	String myPort=null;
	Context con;

	ArrayList<String> arr = new ArrayList<String>();
	HashMap<String, String> hm = new HashMap<String, String>();
	HashMap<String, String> local = new HashMap<String, String>();
	TreeMap<String, String> hashPortMap = new TreeMap<String, String>();

	String pred=null;
	String pred2=null;
	String succ=null;
	String succ2=null;

	String myId;
	String avd;
	String portStr;
	String hashPort=null;
	Boolean recoveredAvd=false;
//	SharedPreferences sharedPref = getPreferences(Context.MODE_PRIVATE);

	ReentrantLock lock=new ReentrantLock();

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
//		Log.i(TAG,"Delete		:		");
//		try{
//			if(local.containsKey(selection)){
//				local.remove(selection);
//			}
//		}
//		catch(Exception e){
//			Log.e(TAG,"Delete		:		Couldn't delete msges "+e.getMessage());
//		}

		try {

				Log.i(TAG,"In Delete");
				String keySelectionHash = genHash(selection);
				String destAvdHash = getPosition(keySelectionHash);
				String currentAvdHash = genHash(portStr);

				String port1 = String.valueOf(Integer.parseInt(hashPortMap.get(succ))*2);
				String port2 = String.valueOf(Integer.parseInt(hashPortMap.get(succ2))*2);

				Log.i(TAG,"key    "+selection);
				Log.i(TAG,"Hash of key to be deleted    "+genHash(selection));
				Log.i(TAG,"Current avd ->"+currentAvdHash+"---"+"destAvd ->"+destAvdHash);


				if(destAvdHash.equals(currentAvdHash))
				{
					Log.i(TAG,"Case 1");
					Log.i(TAG,"primary "+currentAvdHash);

					//deleteion -> primary node
					for (Map.Entry<String, String> entry : local.entrySet()) {
						System.out.println("Before-----"+entry.getKey() + " => " + entry.getValue());
					}

					lock.lock();
					local.remove(selection);
					lock.unlock();

					for (Map.Entry<String, String> entry : local.entrySet()) {
						System.out.println("After-----"+entry.getKey() + " => " + entry.getValue());
					}

					//deleteion -> succ
					try {
						Log.i(TAG,"Succ "+port1);
						Socket socket = null;
//						String port = String.valueOf(Integer.parseInt(hashPortMap.get(succ)) * 2);

						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port1));
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Object[] o = new Object[]{"D", selection};
						Log.i(TAG, " object written " + o[0]+" , "+o[1]);
						oos.writeObject(o);
						Log.d(TAG, "*********** Wrote object msgToSend: (Sent delete) - <" + selection + ">");
						oos.flush();
//                oos.close();
					}
					catch (Exception e){
						Log.e(TAG,"Couldn't delete "+e.getMessage());
					}

					//deleteion -> succ2
					try {
						Log.i(TAG,"Succ2 "+port2);
						Socket socket = null;
//						String port = String.valueOf(Integer.parseInt(hashPortMap.get(succ2)) * 2);

						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port2));
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Object[] o = new Object[]{"D", selection};
						Log.i(TAG, " object written " + o[0]+" , "+o[1]);
						oos.writeObject(o);
						Log.d(TAG, "*********** Wrote object msgToSend: (Sent delete) - <" + selection + ">");
						oos.flush();
//                oos.close();

					}
					catch (Exception e){
						Log.e(TAG,"Couldn't delete "+e.getMessage());
					}

				}
				else{

					String succDestAvd = getSucc(destAvdHash);
					String succ2DestAvd = getSucc2(destAvdHash);
					Log.i(TAG,"Succ Dest Avd"+succDestAvd);
					Log.i(TAG,"Succ2 Dest Avd"+succ2DestAvd);

					String succPort = String.valueOf(Integer.parseInt(hashPortMap.get(succDestAvd))*2);
					String succ2Port = String.valueOf(Integer.parseInt(hashPortMap.get(succ2DestAvd))*2);
					Log.i(TAG,"Port of Succ Dest Avd"+succPort);
					Log.i(TAG,"Port of Succ2 Dest Avd"+succ2Port);


					try{
						Socket socket = null;
						String port = String.valueOf(Integer.parseInt(hashPortMap.get(destAvdHash))*2);
						Log.i(TAG,"port"+port);


						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Object[] o = new Object[]{"D", selection};
						Log.i(TAG, " object written " + o[0]+" , "+o[1]);
						oos.writeObject(o);
						Log.d(TAG, "*********** Wrote object msgToSend: (Sent delete) - <" + selection + ">");
						oos.flush();
//                oos.close();

					}catch (Exception e){
						Log.e(TAG,"Couldn't delete "+e.getMessage());
					}

					try{
						Socket socket = null;
//						String port = String.valueOf(Integer.parseInt(hashPortMap.get(succPort))*2);
//						Log.i(TAG,"port"+port);


						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Object[] o = new Object[]{"D", selection};
						Log.i(TAG, " object written " + o[0]+" , "+o[1]);
						oos.writeObject(o);
						Log.d(TAG, "*********** Wrote object msgToSend: (Sent delete) - <" + selection + ">");
						oos.flush();
//                oos.close();

					}catch (Exception e){
						Log.e(TAG,"Couldn't delete "+e.getMessage());
					}

					try{
						Socket socket = null;
//						String port = String.valueOf(Integer.parseInt(hashPortMap.get(succ2Port))*2);
//						Log.i(TAG,"port"+port);


						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succ2Port));
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Object[] o = new Object[]{"D", selection};
						Log.i(TAG, " object written " + o[0]+" , "+o[1]);
						oos.writeObject(o);
						Log.d(TAG, "*********** Wrote object msgToSend: (Sent delete) - <" + selection + ">");
						oos.flush();
//                oos.close();
					}catch (Exception e){
						Log.e(TAG,"Couldn't delete "+e.getMessage());
					}
				}


		} catch (Exception e) {
			Log.e(TAG, "ClientTask UnknownHostException :  " + e.getMessage());
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public String getPosition(String hs){
		Log.i(TAG, "In getPosition		:		Array elements: "+arr);
		Log.i(TAG, "In getPosition		:		Key Hash: "+hs);
		if(hs.compareTo(arr.get(0))<0){
			return arr.get(0);
		}
		else if(hs.compareTo(arr.get(arr.size()-1))>0){
			return arr.get(0);
		}
		else
		{
			for(int i=1; i<arr.size(); i++) {
				if(hs.compareTo( arr.get(i-1) )>0 && hs.compareTo( arr.get(i) )<=0 ) {
					return arr.get(i);
				}
			}
		}

		return null;
	}

	public void setPredSucc(ArrayList<String> arr){
		int index = arr.indexOf(hashPort);
		int size = arr.size();

		//succ
		if(index+1==size) {
			succ=arr.get(0);
		}
		else {
			succ=arr.get(index+1);
		}

		//pred
		if(index==0) {
			pred=arr.get(size-1);
		}

		else{
			pred=arr.get(index-1);
		}

		//succ2
		if(index+1==size) {
			succ2 = arr.get(1);
		}
		else if(index+2==size) {
			succ2=arr.get(0);
		}

		else {
			succ2=arr.get(index+2);
		}

		//pred2
		if(index==0) {
			pred2 = arr.get(size-2);
		}
		else if(index==1) {
			pred2=arr.get(size-1);
		}

		else {
			pred2=arr.get(index-2);
		}

	}

	public String getPort(String hash)
	{

		for(int i=0; i< plist.length; i++)
		{
			try{
				if(genHash(plist[i]).equals(hash)){
					Log.i(TAG,"Ports[i]"+ports[i]);
					return ports[i];
				}
			}
			catch (Exception e){
				Log.e(TAG,e.getMessage());
			}
		}
		return null;
	}

	public String getSucc2(String hs){
		int size = arr.size();
		int index = arr.indexOf(hs);

		if(index==size-2){
			return arr.get(0);
		}
		else if(index==size-1){
			return arr.get(1);
		}
		else
		{
			return arr.get(index+2);
		}

	}

	public String getPred2(String hs){
		int size = arr.size();
		int index = arr.indexOf(hs);

		if(index==0){
			return arr.get(size-2);
		}
		else if(index==1){
			return arr.get(size-1);
		}
		else
		{
			return arr.get(index-2);
		}

	}

	public String getSucc(String hs){
		int index = arr.indexOf(hs);
		int size = arr.size();

		if(index==size-1){
			return arr.get(0);
		}
		else
		{
			return arr.get(index+1);
		}
	}

	public String getPred(String hs){
		int index = arr.indexOf(hs);
		int size = arr.size();

		if(index==0){
			return arr.get(size-1);
		}
		else
		{
			return arr.get(index-1);
		}
	}

	public void sendText(String msg, String port)
	{
		Socket socket = null;
		try {
			String[] m = msg.split(":");
			String identifier = m[0];
			String k=m[1];
			String v=m[2];

			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			Object[] o = new Object[]{identifier, k, v};
			oos.writeObject(o);
			Log.d(TAG, "*********** In senTest: Wrote object msgToSend: (Sent insert) - <" + k + v + ">");
//                oos.flush();
//                oos.close();
		} catch (Exception e) {
			Log.e(TAG,"Couldn't create socket in insert"+e.getMessage());
		}
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String k = (String) values.get("key");
		String v = values.getAsString("value");
		String keyHash="";
		try {
			keyHash = genHash(k);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		Log.i(TAG,"In insert		:		arr "+arr);
		String destAvdHash = getPosition(keyHash);
		String str = "I:"+k+":"+v;
		Log.d(TAG, "In insert		:		Message to be saved: " + str);
		Log.d(TAG, "In insert		:		Hash where the value needs to be saved: " + destAvdHash);

		String port = String.valueOf(Integer.parseInt(hashPortMap.get(destAvdHash))*2);
		Log.d(TAG, "In insert		:		Port corresponding to the hash: " + destAvdHash + " is: " + port);

		String port1 = String.valueOf(Integer.parseInt(hashPortMap.get(succ))*2);
		Log.d(TAG, "In insert		:		Port corresponding to port1 (Succ) of " + destAvdHash + " is : " + port1);

		String port2 = String.valueOf(Integer.parseInt(hashPortMap.get(succ2))*2);
		Log.d(TAG, "In insert		:		Port corresponding to port2 (Succ2) of " + destAvdHash + " is : " + port2);

		String succDestAvd = getSucc(destAvdHash);
		String succ2DestAvd = getSucc2(destAvdHash);
		Log.i(TAG,"Succ Dest Avd"+succDestAvd);
		Log.i(TAG,"Succ2 Dest Avd"+succ2DestAvd);

		String succPort = String.valueOf(Integer.parseInt(hashPortMap.get(succDestAvd))*2);
		String succ2Port = String.valueOf(Integer.parseInt(hashPortMap.get(succ2DestAvd))*2);
		Log.i(TAG,"Port of Succ Dest Avd"+succPort);
		Log.i(TAG,"Port of Succ2 Dest Avd"+succ2Port);

		Log.i(TAG,"portStr"+portStr);


		if(port.equals(myPort)){
			try{
				Log.i(TAG,"Inside equals(Insert) - primary");
				lock.lock();
				local.put(k,v);
				lock.unlock();
			}
			catch (Exception e) {
				Log.e(TAG,"Inside insert - avd is dead!"+e.getMessage());
			}
				//send to succ -> create soc with succ
			try {
				Log.i(TAG,"Inside equals(Insert) -succ1");
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port1));
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				Object[] o = new Object[]{"I", k, v};
				oos.writeObject(o);
				Log.d(TAG, "*********** Wrote object msgToSend: (Sent insert) - <" + o[0]+"---"+ o[1]+"---"+o[2]+"> to "+port1);
//                oos.flush();
//                oos.close();

			} catch (Exception e) {
				Log.e(TAG,"Couldn't create socket in insert"+e.getMessage());
			}
				//send to succ2 ->
			try {
				Log.i(TAG,"Inside equals(Insert) - succ2");
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port2));
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				Object[] o = new Object[]{"I", k, v};
				oos.writeObject(o);
				Log.d(TAG, "*********** Wrote object msgToSend: (Sent insert) - <" + o[0]+"---"+ o[1]+"---"+o[2]+ "> to "+port2);
//                oos.flush();
//                oos.close();
			} catch (Exception e) {
				Log.e(TAG,"Couldn't create socket in insert"+e.getMessage());
			}
			}
			else{
//				sendText(str,port); // 3 sockets here only
			//send to succ -> create soc with succ
			//send to succ2 -> "

			//Primary node
//			Socket socket = null;
			try {
				Log.i(TAG,"Inside else(Insert) ");
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				Object[] o = new Object[]{"I", k, v};
				oos.writeObject(o);
				Log.d(TAG, "********** Wrote object msgToSend: (Sent insert) - <" + o[0]+"---"+ o[1]+"---"+o[2]+ "> to "+port);
//                oos.flush();
//                oos.close();
			} catch (Exception e) {
				Log.e(TAG,"Couldn't create socket in insert"+e.getMessage());
			}
			}

			//successor 1
//			Socket socket = null;
			try {

			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			Object[] o = new Object[]{"I", k, v};
			oos.writeObject(o);
			Log.d(TAG, "*********** Wrote object msgToSend: (Sent insert) - <" + o[0]+"---"+ o[1]+"---"+o[2]+ "> to "+succPort);
//                oos.flush();
//                oos.close();
			} catch (Exception e) {
			Log.e(TAG,"Couldn't create socket in insert"+e.getMessage());
			}

		//successor 2
//		Socket socket = null;
		try {

			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succ2Port));
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			Object[] o = new Object[]{"I", k, v};
			oos.writeObject(o);
			Log.d(TAG, "***********   Wrote object msgToSend: (Sent insert) - <" + o[0]+"---"+ o[1]+"---"+o[2]+ "> to "+succ2Port);
//                oos.flush();
//                oos.close();
		} catch (Exception e) {
			Log.e(TAG,"Couldn't create socket in insert"+e.getMessage());
		}

		return uri;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		String message;
//        ObjectInputStream readObj=null;

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Log.i(TAG,"Server		:		In server task");
//            Socket soc=null;
			while (true) {

				try {
					Socket soc = serverSocket.accept();
					Log.d(TAG, "Server		:		Socket created");
					ObjectInputStream readObj = new ObjectInputStream(soc.getInputStream());
					Log.d(TAG, "Server		:		Input stream created");
					Object[] obj = (Object[]) readObj.readObject();
					Log.d(TAG, "Server			:			First component of Object Read: " + obj[0]);
//                    String[] str = ((String) obj[0]).split(":");
					String idn=(String) obj[0];
					Log.i(TAG,"Server			:			Identifier :"+idn);

					if (idn.equals("I")) {
						Log.i(TAG,"Server		:		In Insert");
						String k=(String)obj[1];
						String v=(String)obj[2];

						lock.lock();
						local.put(k,v);
						lock.unlock();
						Log.i("Server		:		KV Inserted","<"+k+" , "+v+"> inserted in "+portStr);

					}
					if (idn.equals("D")) {
						Log.i(TAG,"Server		:		In Delete");
						String k=(String)obj[1];

						lock.lock();
						local.remove(k);
						lock.unlock();
						Log.i("Server		:		KV Deleted","<"+k+" , "+"> inserted in "+portStr);

					}
					if (idn.equals("Q")) {
						Log.i(TAG, "Server		:		In Query");


						try {
							Log.i(TAG,"Server		:		in server of query");
							ObjectOutputStream oos2 = new ObjectOutputStream(soc.getOutputStream());

							Log.i(TAG,"Server		:		local"+local);
//							lock.lock();
							Object[] o = new Object[]{local};

//                                Log.i(TAG,"object written"+o[1]);
							oos2.writeObject(o);
//							lock.unlock();
//                                oos2.flush();
//                                oos2.close();

						} catch (Exception e) {
							Log.e(TAG, "Server		:		Couldn't create socket :  " + e.getMessage());


						}
					}

				} catch (IOException e) {
					Log.e(TAG, "Server		:		Couldn't create serverSocket in server task "+e.getMessage());
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					Log.e(TAG, "Server		:		Couldn't create serverSocket in server task "+e.getMessage());
					e.printStackTrace();
				}
			}



		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			Log.i(TAG, "Client		:		In Client task");
//			String[] m = msgs[0].split(":");
			String flag = msgs[0];
			Log.i(TAG, "Client		:		Flag" + flag);


			if (flag.equals("QP1")){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
					Log.i(TAG,"Client(QP1)		:		port    "+msgs[1]);
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					oos.writeObject(o);
//                        oos.flush();

					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Client		:		Object received from server  "+obj[0]);
					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						System.out.println("Client		:		"+entry.getKey() + " => " + entry.getValue());
						Log.i(TAG,"Client		:		Checking -> pred2: "+pred2+" getPosition(genHash(entry.getKey())): "+getPosition(genHash(entry.getKey()))+" key: "+entry.getKey());
						if(pred2.equals(getPosition(genHash(entry.getKey())))){
							Log.i(TAG,"Client		:		Key matched!   -> "+getPosition(genHash(entry.getKey()))+entry.getKey());
							local.put(entry.getKey(), entry.getValue());
						}
					}
					Log.i(TAG,"Client(After recovery)			: local -> "+local);
				} catch (Exception e) {
					Log.e(TAG, "Client		:		Couldn't create Socket " + e.getMessage());
				}
			}

			if (flag.equals("QS1")){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
					Log.i(TAG,"Client(QS1)		:		port    "+msgs[1]);
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					oos.writeObject(o);
//                        oos.flush();

					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Client		:		Object received from server  "+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						System.out.println("Client		:		"+entry.getKey() + " => " + entry.getValue());
						Log.i(TAG,"Client		:		Checking -> pred: "+pred+" getPosition(genHash(entry.getKey())): "+getPosition(genHash(entry.getKey()))+" key: "+entry.getKey());
						if(pred.equals(getPosition(genHash(entry.getKey())))){
							Log.i(TAG,"Client		:		Key matched!   -> "+getPosition(genHash(entry.getKey()))+entry.getKey());
							local.put(entry.getKey(), entry.getValue());
						}
					}
					Log.i(TAG,"Client(After recovery)			: local -> "+local);
				} catch (Exception e) {
					Log.e(TAG, "Client		:		Couldn't create Socket " + e.getMessage());
				}
			}

			if (flag.equals("QS2")){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
					Log.i(TAG,"Client(QS2)		:		port    "+msgs[1]);
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					oos.writeObject(o);
//                        oos.flush();

					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Client		:		Object received from server  "+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						System.out.println("Client		:		"+entry.getKey() + " => " + entry.getValue());
						Log.i(TAG,"Client		:		Checking -> myId: "+myId+" getPosition(genHash(entry.getKey())): "+getPosition(genHash(entry.getKey()))+" key: "+entry.getKey());
						if(myId.equals(getPosition(genHash(entry.getKey())))){
							Log.i(TAG,"Client		:		Key matched!  -> "+getPosition(genHash(entry.getKey()))+entry.getKey());
							local.put(entry.getKey(), entry.getValue());
						}
					}
					Log.i(TAG,"Client(After recovery)			: local -> "+local);
				} catch (Exception e) {
					Log.e(TAG, "Client		:		Couldn't create Socket " + e.getMessage());
				}
			}
			return null;
		}
	}

	public synchronized void recovery(){
		//ask from pred

		Log.i(TAG,"Recovery		:		In recovery");
		try{
			String msg="QP1";
			Log.i(TAG,"Recovery		:		MyId : "+myId);
			String predecessor = getPred(myId);
			Log.i(TAG,"Recovery		:		Predecessor  "+predecessor);
			String port = String.valueOf(Integer.parseInt(hashPortMap.get(predecessor))*2);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
//			Log.i(TAG,"Recovery		:		"+port);
//				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
//				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//				Object[] o = new Object[]{"Q"};
//			Log.i(TAG,"Recovery		:		Wrote Object "+o);
//				oos.writeObject(o);
////                        oos.flush();
//
//				ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
//				Object[] obj = (Object[]) readObj.readObject();
//				Log.i(TAG,"Client		:		Object received from server  "+obj[0]);
//				HashMap<String, String> hm = (HashMap<String, String>) obj[0];
//				for (Map.Entry<String, String> entry : hm.entrySet()) {
//					System.out.println("Client		:		"+entry.getKey() + " => " + entry.getValue());
//					Log.i(TAG,"Client		:		Checking -> pred2: "+pred2+" getPosition(genHash(entry.getKey())): "+getPosition(genHash(entry.getKey()))+" key: "+entry.getKey());
//					if(pred2.equals(getPosition(genHash(entry.getKey())))){
//						Log.i(TAG,"Client		:		Key matched!   -> "+getPosition(genHash(entry.getKey())));
//						local.put(entry.getKey(), entry.getValue());
//					}
//				}

		}catch (Exception e){
			Log.i(TAG,"Recovery		:		Error (pred)--- "+e);
		}

		try{
			String msg="QS1";
			Log.i(TAG,"Recovery		:		MyId : "+myId);
			String successor = getSucc(myId);
			Log.i(TAG,"Recovery		:		Successor  "+successor);
			String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor))*2);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
//			Log.i(TAG,"Recovery		:		"+port);
//
//				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
//				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//				Object[] o = new Object[]{"Q"};
//			Log.i(TAG,"Recovery		:		Wrote Object "+o);
//				oos.writeObject(o);
////                        oos.flush();
//
//				ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
//				Object[] obj = (Object[]) readObj.readObject();
//				Log.i(TAG,"Client		:		Object received from server  "+obj[0]);
//
//				HashMap<String, String> hm = (HashMap<String, String>) obj[0];
//				for (Map.Entry<String, String> entry : hm.entrySet()) {
//					System.out.println("Client		:		"+entry.getKey() + " => " + entry.getValue());
//					Log.i(TAG,"Client		:		Checking -> pred: "+pred+" getPosition(genHash(entry.getKey())): "+getPosition(genHash(entry.getKey()))+" key: "+entry.getKey());
//					if(pred.equals(getPosition(genHash(entry.getKey())))){
//						Log.i(TAG,"Client		:		Key matched!   -> "+getPosition(genHash(entry.getKey())));
//						local.put(entry.getKey(), entry.getValue());
//					}
//				}

		}catch (Exception e){
			Log.i(TAG,"Recovery		:		Error (succ)--- "+e.getMessage());
		}

		try{
			String msg="QS2";
			Log.i(TAG,"Recovery		:		MyId : "+myId);
			String successor2 = getSucc2(myId);
			Log.i(TAG,"Recovery		:		Successor2  "+successor2);
			String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor2))*2);
			Log.i(TAG,"Recovery		:		"+port);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);

//				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
//				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//				Object[] o = new Object[]{"Q"};
//			Log.i(TAG,"Recovery		:		Wrote Object "+o);
//				oos.writeObject(o);
////                        oos.flush();
//
//				ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
//				Object[] obj = (Object[]) readObj.readObject();
//				Log.i(TAG,"Client		:		Object received from server  "+obj[0]);
//
//				HashMap<String, String> hm = (HashMap<String, String>) obj[0];
//				for (Map.Entry<String, String> entry : hm.entrySet()) {
//					System.out.println("Client		:		"+entry.getKey() + " => " + entry.getValue());
//					Log.i(TAG,"Client		:		Checking -> myId: "+myId+" getPosition(genHash(entry.getKey())): "+getPosition(genHash(entry.getKey()))+" key: "+entry.getKey());
//					if(myId.equals(getPosition(genHash(entry.getKey())))){
//						Log.i(TAG,"Client		:		Key matched!  -> "+getPosition(genHash(entry.getKey())));
//						local.put(entry.getKey(), entry.getValue());
//					}
//				}

		}catch (Exception e){
			Log.i(TAG,"Recovery		:		Error (succ2)--- "+e.getMessage());
		}

	}
	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		lock.lock();
		Log.i(TAG,"OnCreate		:		In OnCreate");
		Context context=getContext();
		SharedPreferences sharedPref = context.getSharedPreferences("recovery",Context.MODE_PRIVATE);
		recoveredAvd = sharedPref.getBoolean("recoveredAvd", false);
		SharedPreferences.Editor editor = sharedPref.edit();
		editor.putBoolean("recoveredAvd",true);
		editor.commit();

		try
		{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			Log.i(TAG,"OnCreate		:		Created ServerSocket");
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (IOException e)
		{
			Log.e(TAG, "OnCreate		:		Can't create a ServerSocket");
			Log.e(TAG, e.getMessage());
		}


		TelephonyManager tel = (TelephonyManager) context.getSystemService(context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));    //11108

		try {
			hashPort=genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		for(String str: plist)
		{
			try {
				hashPortMap.put(genHash(str), str);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		Log.i(TAG, "OnCreate		:		portStr : "+portStr);
		Log.i(TAG,"OnCreate		:		hashPortMap : "+hashPortMap);


		//For every avd->call the client task


		try
		{
			Log.i(TAG,"OnCreate		:		portStr"+portStr);
			myId =genHash(portStr);
			Log.i(TAG,"OnCreate		:		myId"+myId);
//			pred=succ=myId;
			String str = "J:"+myId;

			Log.i(TAG,"OnCreate		:		Current avd : portStr->"+portStr+ "    hash ->"+myId);

			arr.add("177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
			arr.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");
			arr.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");
			arr.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");
			arr.add("c25ddd596aa7c81fa12378fa725f706d54325d12");

			setPredSucc(arr);

			Log.i(TAG,"OnCreate		:		Array - "+arr);
			Log.i(TAG,"OnCreate		:		Predecessor : "+pred);
			Log.i(TAG,"OnCreate		:		Successor1 : "+succ);
			Log.i(TAG,"OnCreate		:		Successor2 : "+succ2);


		}
		catch(Exception e){
			Log.e(TAG,"OnCreate		:		Client Task can't be called!"+e.getMessage());
		}

		if(recoveredAvd){
			Log.i(TAG,"OnCreate		:		In recovery!!");
			//start normal execution
			recovery();
		}
		lock.unlock();
		return true;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.i(TAG,"Query		:		In Query");

		MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
		//        MatrixCursor.RowBuilder newRow = matrixCursor.newRow();
//		con = getContext();

		try {
			if (selection.equals("@")) {
				Log.i(TAG, "Query		:		In @");
				lock.lock();
				for (Map.Entry<String, String> entry : local.entrySet()) {
					System.out.println(entry.getKey() + " => " + entry.getValue());
					Log.i(TAG, "Query		:		SELECTION : " + selection);
					Log.i(TAG, "Query		:		"+entry.getKey() + " => " + entry.getValue());
					matrixCursor.newRow().add("key",entry.getKey()).add("value",entry.getValue());
				}
				lock.unlock();
			}
			else if(selection.equals("*")){
				Log.i(TAG,"Query		:		In *");
				try{
					String successor2 = getSucc2(arr.get(0));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor2))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query  "+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server  "+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

//					socket.close();

				}catch (Exception e){
					Log.e(TAG,"Query		:		Avd is not alive i guess! "+e.getMessage());
					//get data from successor

					String successor1 = getSucc(arr.get(0));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor1))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query "+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server "+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

				}

				try{
					String successor2 = getSucc2(arr.get(1));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor2))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query"+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server"+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

//					socket.close();

				}catch (Exception e){
					Log.e(TAG,"Query		:		Avd is not alive i guess!"+e.getMessage());

					String successor1 = getSucc(arr.get(1));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor1))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query"+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server"+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

				}

				try{
					String successor2 = getSucc2(arr.get(2));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor2))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query"+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server"+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

//					socket.close();

				}catch (Exception e){
					Log.e(TAG,"Query		:		Avd is not alive i guess!"+e.getMessage());

					String successor1 = getSucc(arr.get(2));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor1))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query"+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server"+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

				}

				try{
					String successor2 = getSucc2(arr.get(3));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor2))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query"+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server"+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

//					socket.close();

				}catch (Exception e){
					Log.e(TAG,"Query		:		Avd is not alive i guess!"+e.getMessage());

					String successor1 = getSucc(arr.get(3));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor1))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query"+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server"+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

				}

				try{

					String successor2 = getSucc2(arr.get(4));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor2))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query"+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server"+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						System.out.println("Query		:		"+entry.getKey() + " => " + entry.getValue());
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

//					socket.close();

				}catch (Exception e){
					Log.e(TAG,"Query		:		Avd is not alive i guess!"+e.getMessage());

					String successor1 = getSucc(arr.get(4));
					String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor1))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					Object[] o = new Object[]{"Q"};
					Log.i(TAG,"Query		:		Object written from query"+o[0]);
					oos.writeObject(o);
//					oos.flush();
//                        oos.close();

					//Reading from a socket
					ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
					Object[] obj = (Object[]) readObj.readObject();
					Log.i(TAG,"Query		:		Object received from server"+obj[0]);

					HashMap<String, String> hm = (HashMap<String, String>) obj[0];
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
					}

				}



			}
			else {
				Log.i(TAG,"Query		:		In key(case 3) of query");
				String keySelectionHash = genHash(selection);
				String destAvdHash = getPosition(keySelectionHash);
				String currentAvdHash = genHash(portStr);
				String readAvdHash = getSucc2(destAvdHash);
				Log.i(TAG,"Query		:		key"+selection);
				Log.i(TAG,"Query		:		Hash of key"+genHash(selection));

				Log.i(TAG,"Query		:		Current avd ->"+currentAvdHash+"---"+"destAvd ->"+destAvdHash+"---"+"Succ2Avd ->"+readAvdHash);

				//destAvdHash ki jgh succ2(if alive)->try catch
				if(destAvdHash.equals(currentAvdHash))
				{
					try {
						Log.i(TAG, "Query		:		Case 1");
						Log.i(TAG, "Query		:		Succ2  " + succ2);
						String port = String.valueOf(Integer.parseInt(hashPortMap.get(succ2)) * 2);
						Log.i(TAG, "Query		:		Port of Succ2  " + port);

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Object[] o = new Object[]{"Q"};
						Log.i(TAG, "Query		:		object written " + o[0]);
						oos.writeObject(o);
						Log.d(TAG, "Query		:		(Sent to query)key-> <" + selection + ">");
//					oos.flush();
//                oos.close();

						ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
						Log.i(TAG, "Query		:		Read stream created1");
						Log.i(TAG,"Query		:		Making connection with succ 2 :"+succ2+"  PORT  -> "+port);
						Object[] obj = (Object[]) readObj.readObject();
						Log.i(TAG, "Query		:		Read object" + obj[0]);

						HashMap<String, String> hm = (HashMap<String, String>) obj[0];
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", selection).add("value", hm.get(selection));
					}
					catch (Exception e){
						// get successor's data
						Log.e(TAG,"Query		:		Avd failed in query case 3");
						Log.i(TAG, "Query		:		Succ  " + succ);
						String port = String.valueOf(Integer.parseInt(hashPortMap.get(succ)) * 2);
						Log.i(TAG, "Query		:		Port of Succ2  " + port);

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Object[] o = new Object[]{"Q"};
						Log.i(TAG, "Query		:		object written " + o[0]);
						oos.writeObject(o);
						Log.d(TAG, "Query		:		(Sent to query)key-> <" + selection + ">");
//					oos.flush();
//                oos.close();

						ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
						Log.i(TAG, "Query		:		Read stream created2");
						Log.i(TAG,"Query		:		Making connection with successor  :"+succ+"  PORT  -> "+port);
						Object[] obj = (Object[]) readObj.readObject();
						Log.i(TAG, "Query		:		Read object" + obj[0]);

						HashMap<String, String> hm = (HashMap<String, String>) obj[0];
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", selection).add("value", hm.get(selection));

					}
				}
				else{
					try {
						String port = String.valueOf(Integer.parseInt(hashPortMap.get(readAvdHash)) * 2);
						Log.i(TAG, "Query		:		port" + port);

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Object[] o = new Object[]{"Q"};
						Log.i(TAG, "Query		:		object written " + o[0]);
						oos.writeObject(o);
						Log.d(TAG, "Query		:		(Sent to query)key-> <" + selection + ">");
//					oos.flush();
//                oos.close();
						ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
						Log.i(TAG, "Query		:		Read stream created3");
						Log.i(TAG,"Query		:		Making connection with successor 2 :"+readAvdHash+"  PORT  -> "+port);
						Object[] obj = (Object[]) readObj.readObject();
						Log.i(TAG, "Query		:		Read object" + obj[0]);

						HashMap<String, String> hm = (HashMap<String, String>) obj[0];
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", selection).add("value", hm.get(selection));
					}catch (Exception e){
						Log.e(TAG,"Query		:		Avd is not alive"+e.getMessage());
						String successor = getSucc(destAvdHash);

						String port = String.valueOf(Integer.parseInt(hashPortMap.get(successor)) * 2);
						Log.i(TAG, "Query		:		port" + port);

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Object[] o = new Object[]{"Q"};
						Log.i(TAG, " =Query		:		object written " + o[0]);
						oos.writeObject(o);
						Log.d(TAG, "Query		:		(Sent to query)key-> <" + selection + ">");
//					oos.flush();
//                oos.close();
						ObjectInputStream readObj = new ObjectInputStream(socket.getInputStream());
						Log.i(TAG, "Query		:		Read stream created4");
						Object[] obj = (Object[]) readObj.readObject();
						Log.i(TAG, "Query		:		Read object" + obj[0]);

						HashMap<String, String> hm = (HashMap<String, String>) obj[0];
						for (Map.Entry<String, String> entry : hm.entrySet()) {
							System.out.println("Query		:		"+entry.getKey() + " => " + entry.getValue());
//                                    if(selection.equals(entry.getKey())){
//                                        matrixCursor.newRow().add("key", entry.getKey()).add("value", entry.getValue());
//                                    }
						}
						Log.i(TAG,"key : "+selection+"   value : "+hm.get(selection));
						matrixCursor.newRow().add("key", selection).add("value", hm.get(selection));

					}
				}

			}
		} catch (UnknownHostException e) {
			Log.e(TAG, "Query		:		ClientTask UnknownHostException :  " + e.getMessage());
		} catch (IOException e) {
			Log.e(TAG, "Query		:		ClientTask socket IOException :  " + e.getMessage());
		} catch (Exception e) {
			Log.e(TAG, e.getMessage());
		}

		Log.i(TAG,"Query		:		Query Ends!!!");
		return matrixCursor;
//		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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
}
