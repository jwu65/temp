package org.pdcl.mcast;

import java.io.*;
import java.net.*;
import java.util.LinkedList;

public class MulticastReceiver {

  public static void main(String[] args) throws Exception {
    MulticastReceiver multiReceiver = new MulticastReceiver();
    multiReceiver.run(args);
    return;
  }

  NetworkInterface mcastNif = null;
  InetAddress mcastAddr = null;
  int mcastPort = -1;
  int ucastPort = -1;
  LinkedList<DatagramPacket> ackQueue = null;

  public MulticastReceiver() throws Exception {
    mcastNif = NetworkInterface.getByName("eth1");
    mcastAddr = InetAddress.getByName("228.8.8.7");
    mcastPort = 52222;
    ucastPort = 51111;
    ackQueue = new LinkedList<DatagramPacket>();
  }
  
  public void run(String[] args) throws Exception {

    ServerSocket serverSock = new ServerSocket(ucastPort);
    DesController controller = new DesController( serverSock);

    controller.start();

    controller.join();
  }

  private class DesController extends Thread {

    private volatile boolean closed = false;
    ServerSocket serverSock = null;
    Socket srcSock = null;
    Socket desSock = null;

    DesController( ServerSocket serverSock){
      this.serverSock = serverSock;
    }

    public void run(){

      int headerSize = -1;
      int payloadSizeMax = -1;
      int checksumSize = -1;
      int numDes = -1;
      int numPkt = -1;
      int op = -1;
      String[] desList = null;

      DataInputStream srcIn = null; 
      DataOutputStream srcOut = null;
      DataInputStream desIn = null; 
      DataOutputStream desOut = null;

      DataReceiver receiver = null;

      try{
        srcSock = serverSock.accept();
        srcIn= new DataInputStream(srcSock.getInputStream());
        srcOut= new DataOutputStream(srcSock.getOutputStream());


        op = srcIn.readInt();

        if(op == 10){
          headerSize = srcIn.readInt();
          payloadSizeMax = srcIn.readInt();
          checksumSize = srcIn.readInt();
          numPkt = srcIn.readInt();
          numDes = srcIn.readInt();
          desList = new String[numDes];

          System.out.println("hd"+headerSize+" pl"+payloadSizeMax+" ck"+checksumSize+" np"+numPkt+" nd"+numDes);

          if(numDes > 0){
            for(int i=0; i<numDes; i++){
              desList[i] = srcIn.readUTF();
              System.out.println("des"+i+" = "+desList[i]);
            }

            desSock = new Socket( desList[0], ucastPort);
            desIn = new DataInputStream(desSock.getInputStream());
            desOut= new DataOutputStream(desSock.getOutputStream());
        
            desOut.writeInt(10);
            desOut.writeInt(headerSize);
            desOut.writeInt(payloadSizeMax);
            desOut.writeInt(checksumSize);
            desOut.writeInt(numPkt);
            desOut.writeInt(numDes - 1);
            for( int i=1; i<numDes; i++){
              desOut.writeUTF(desList[i]);
            }
            desOut.flush();

            op = desIn.readInt();
            if(op != 100){
              throw new Exception("Wrong OP");
            }
          }

          receiver = new DataReceiver( headerSize, payloadSizeMax, checksumSize, numPkt);
          receiver.start();

          srcOut.writeInt(100);
          srcOut.flush();

        }
        else if(op == 0){
        }
        else{
          throw new Exception("Wrong OP");
        }
        
        closed = true;
        System.out.println("Controller finished.");



        if(receiver != null){
          receiver.join();
        }

      }catch(Exception e){
        e.printStackTrace();
      }finally{
        try{
          if(desIn != null)
            desIn.close();
          if(desOut != null)
            desOut.close();
          if(desSock != null)
            desSock.close();
          if(srcIn != null)
            srcIn.close();
          if(srcOut != null)
            srcOut.close();
          if(srcSock != null)
            srcSock.close();
        }catch(Exception e){
        }
      }
    }

    // shutdown thread
    void close() {
      closed = true;
      synchronized (ackQueue) {
        ackQueue.notifyAll();
      }
      this.interrupt();
    }
    
  }


  private class DataReceiver extends Thread {

    private volatile boolean closed = false;
    int headerSize = -1;
    int payloadSizeMax = -1;
    int checksumSize = -1;
    int numPkt = -1;

    DataReceiver( int headerSize, int payloadSizeMax, int checksumSize, int numPkt){
      this.headerSize = headerSize;
      this.payloadSizeMax = payloadSizeMax;
      this.checksumSize = checksumSize;
      this.numPkt = numPkt;
    }

    public void run(){
      MulticastSocket socket = null;
      int pid=0;
      byte[] mbuf = new byte[headerSize + payloadSizeMax + checksumSize];

      try {
        socket = new MulticastSocket(mcastPort);
        socket.setNetworkInterface(mcastNif);
        socket.joinGroup(mcastAddr);
 
        while ( pid < numPkt) {
          DatagramPacket pkt = new DatagramPacket(mbuf, mbuf.length);
          socket.receive(pkt);
          String msg = new String(mbuf, headerSize, pkt.getLength()-headerSize-checksumSize);
          System.out.println("recv seqno:" + pid  + " size:"+(pkt.getLength()-headerSize-checksumSize)+" msg:" + msg);
          pid++;
        }
        socket.leaveGroup(mcastAddr);
      } catch (IOException ioe) {
        System.out.println(ioe);
      }
    }

    // shutdown thread
    void close() {
      closed = true;
      synchronized (ackQueue) {
        ackQueue.notifyAll();
      }
      this.interrupt();
    }
  }
}
