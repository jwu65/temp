package org.pdcl.nsim;

import java.io.*;
import java.util.LinkedList;

public class NetSim {

  public static void main(String[] args) throws Exception {
    NetSim nsim = new NetSim();
    nsim.run(args);
    return;
  }

  int i;

  NetSim() throws Exception {
    i=10;
  }

  void run(String[] args) throws Exception {
    System.out.println("i="+i);
  }

}

//
//public class MulticastSender {
//
//  public static void main(String[] args) throws Exception {
//    MulticastSender multiSender = new MulticastSender();
//    multiSender.run(args);
//    return;
//  }
//
//  NetworkInterface mcastNif = null;
//  InetAddress mcastAddr = null;
//  int mcastPort = -1;
//  int ucastPort = -1;
//  LinkedList<MyPkt> packetQueue = null;
//  LinkedList<MyPkt> processingQueue = null;
//
//  int payloadSizeMax = 512;
//  int headerSize = 4;
//  int checksumSize = 8;
//
//  public MulticastSender() throws Exception {
//    mcastNif = NetworkInterface.getByName("eth1");
//    mcastAddr = InetAddress.getByName("228.8.8.7");
//    mcastPort = 52222;
//    ucastPort = 51111;
//    packetQueue = new LinkedList<MyPkt>();
//    processingQueue = new LinkedList<MyPkt>();
//  }
//  
//  public void run(String[] args) throws Exception {
//
//    /*
//    {
//      long t0, t1;
//
//      FileInputStream fileIn;
//      byte[] fileBuf;
//      t0 = System.nanoTime();
//      try {
//        fileIn = new FileInputStream(args[0]);
//        fileBuf = isTOba(fileIn);
//      } catch (Exception e) {
//        System.out.println(e);
//        return;
//      }
//      t1 = System.nanoTime();
//      System.out.println("Read "+fileBuf.length+" byte in "+(t1-t0)/1e9+"sec.");
//
//      int passCode = 12345;
//      int pos = 0;
//      int seqno = 0;
//      byte[] msgBytes = new byte[headerSize + payloadSizeMax + checksumSize];
//      ByteBuffer bBuf = ByteBuffer.wrap(msgBytes);
//      CRC32 checksum = new CRC32();
//      DatagramPacket mcastPacket = null;
//
//      t0 = System.nanoTime();
//      while(pos < fileBuf.length){
//        int payloadSize;
//
//        bBuf.clear();
//        bBuf.putInt(seqno);
//        payloadSize = Math.min(payloadSizeMax, fileBuf.length - pos);
//        bBuf.put(fileBuf, pos, payloadSize);
//
//        checksum.reset();
//        checksum.update( msgBytes, 0, headerSize+payloadSize);
//        bBuf.putLong(checksum.getValue());
//
//        mcastPacket = new DatagramPacket(bBuf.array(), headerSize+payloadSize+checksumSize, mcastAddr, mcastPort);
//        pos += payloadSize;
//        seqno++;
//      }
//
//      t1 = System.nanoTime();
//      System.out.println("Prepared "+seqno+" packets in "+(t1-t0)/1e9+"sec.");
//    }
//    */
//
//    SrcController controller = new SrcController();
//    controller.start();
//    controller.join();
//    
//
//  }
//
//  private byte[] isTOba(InputStream input)
//      throws IOException {
//    int bytesRead;
//    byte[] buf = new byte[16384];
//    ByteArrayOutputStream output = new ByteArrayOutputStream();
//   
//    bytesRead = input.read(buf);
//    while (bytesRead != -1) {
//      output.write(buf, 0, bytesRead);
//      bytesRead = input.read(buf);
//    }
//    output.flush();
//    
//    return output.toByteArray();
//  }
//
//  public class MyPkt {
//    public int seqno = -1;
//    public DatagramPacket dgpkt = null;
//
//    MyPkt( int seqno, DatagramPacket dgpkt){
//      this.seqno = seqno;
//      this.dgpkt = dgpkt;
//    }
//  }
//
//
//  private class SrcController extends Thread {
//
//    private volatile boolean closed = false;
//    Socket desSock = null;
//
//    SrcController(){
//    }
//
//    public void run(){
//      
//      Socket desSock = null;
//      DataInputStream desIn = null; 
//      DataOutputStream desOut = null;
//      DataSender sender = null;
//
//      int op = -1;
//      int numPkt = 13;
//      int numDes = 2;
//      String[] desList = new String[numDes];
//      desList[0] = "hp3";
//      desList[1] = "hp4";
//      //desList[2] = "hp1";
//
//
//      try{
//        desSock = new Socket( desList[0], ucastPort);
//        desIn= new DataInputStream(desSock.getInputStream());
//        desOut= new DataOutputStream(desSock.getOutputStream());
//        
//        desOut.writeInt(10);
//        desOut.writeInt(headerSize);
//        desOut.writeInt(payloadSizeMax);
//        desOut.writeInt(checksumSize);
//        desOut.writeInt(numPkt);
//        desOut.writeInt(numDes - 1);
//        for( int i=1; i<numDes; i++){
//          desOut.writeUTF(desList[i]);
//        }
//        desOut.flush();
//
//        op = desIn.readInt();
//        if(op != 100){
//          throw new Exception("Wrong OP");
//        }
//
//        System.out.println("Rec OP "+op);
//        sender = new DataSender();
//        sender.start();
//
//      }catch(Exception e){
//        e.printStackTrace();
//      }finally{
//        try{
//          if(desIn != null)
//            desIn.close();
//          if(desOut != null)
//            desOut.close();
//          if(desSock != null)
//            desSock.close();
//        }catch(Exception e){
//        }
//      }
//
//
//
//      int maxPackets = 40;
//      int seqno = 0;
//      String text = "Mcast Msg #";
//      
//
//      while( seqno < numPkt ){
//        byte[] mBuf = (text + seqno).getBytes();
//        ByteBuffer bBuf = ByteBuffer.allocate(headerSize + mBuf.length + checksumSize);
//        CRC32 checksum = new CRC32();
//        
//        synchronized (packetQueue) {
//          while(packetQueue.size() + processingQueue.size() >= maxPackets){
//            try{
//              packetQueue.wait();
//            } catch (InterruptedException  e) {
//            }
//          }
//
//          bBuf.putInt(seqno);
//          bBuf.put(mBuf);
//          checksum.update( bBuf.array(), 0, bBuf.position());
//          bBuf.putLong(checksum.getValue());
//
//          MyPkt mypkt = new MyPkt( seqno, new DatagramPacket(bBuf.array(), 0, headerSize + mBuf.length + checksumSize, mcastAddr, mcastPort));
//
//          seqno++;
//
//          packetQueue.addLast(mypkt);
//          packetQueue.notifyAll();
//        }
//
//        //try {
//        //  Thread.sleep(500);
//        //} catch (InterruptedException ie) {
//        //}
//      }
//
//      synchronized (packetQueue) {
//        while(packetQueue.size() + processingQueue.size() > 0){
//          try{
//            packetQueue.wait();
//          } catch (InterruptedException  e) {
//          }
//        }
//      }
//
//      try{
//        sender.close();
//        sender.join();
//      } catch(Exception e){
//      }
//    }
//  }
//
//  private class DataSender extends Thread {
//
//    private volatile boolean closed = false;
//
//    DataSender(){
//    }
//
//    public void run(){
//      MulticastSocket socket = null;
//      int pid = 0;
//
//      try {
//        socket = new MulticastSocket(mcastPort);
//        socket.setNetworkInterface(mcastNif);
//      } catch (IOException ioe) {
//        System.out.println(ioe);
//      }
//
//      while(!closed){
//        DatagramPacket dgpkt = null;
//
//        synchronized (packetQueue) {
//          while(!closed && packetQueue.size() <= 0){
//            try{
//              packetQueue.wait();
//            } catch (InterruptedException  e) {
//            }
//          }
//          if(closed){
//            continue;
//          }
// 
//          dgpkt = packetQueue.getFirst().dgpkt;
//
//          try {
//            socket.send(dgpkt);
//          } catch (IOException ioe) {
//            System.out.println(ioe);
//          }
// 
//          System.out.println("send seqno:"+ByteBuffer.wrap(dgpkt.getData()).getInt()+" size:"+(dgpkt.getData().length - headerSize - checksumSize)+" msg:"+new String( dgpkt.getData(), headerSize, dgpkt.getData().length - headerSize - checksumSize));
//          packetQueue.removeFirst();
//          packetQueue.notifyAll();
//        }
//      }
//
// 
//    }
//
//    // shutdown thread
//    void close() {
//      closed = true;
//      synchronized (packetQueue) {
//        packetQueue.notifyAll();
//      }
//      //synchronized (ackQueue) {
//      //  ackQueue.notifyAll();
//      //}
//      this.interrupt();
//    }
//
//  }
//
//  private class ResponseProcessor extends Thread {
//
//    private volatile boolean closed = false;
//
//    ResponseProcessor(){
//    }
//
//    public void run(){
//    }
//
//    // shutdown thread
//    void close() {
//      closed = true;
//      this.interrupt();
//    }
//  }
//
//}
