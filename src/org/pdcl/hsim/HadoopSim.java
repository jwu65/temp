package org.pdcl.hsim;

import java.util.Random;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.io.*;

public class HadoopSim {

  public static void main(String[] args) throws Exception {

    HadoopSim hsim = new HadoopSim();
    hsim.run(args);

    return;
  }

  //global var
  Cluster hCluster;
  JobList hJobList;
  Timeline hTimeline;
  TaskPool hTaskPool;
  //global var end


  HadoopSim() throws Exception {
    hCluster = new Cluster();
    hJobList = new JobList();
    hTimeline = new Timeline();
    hTaskPool = new TaskPool();
  }

  void run(String[] args) throws Exception {
   
    hCluster.setAllIdle();
    hCluster.print();

    hJobList.importJobs();

    hTimeline.print();

    System.out.println("\n\n");
  }

  

  private class HadoopJob {
    int id;
    int submitTick;//0.1ms
    String name;
    double inputSize;//KB
    double shuffleSize;
    double outputSize;
    double blockSize;
    int numMapTask;
    int numReduceTask;
    int stage;// 0-created, 

    HadoopJob( int id, String name, int submitTime, long inputBytes, long shuffleBytes, long outputBytes, int numMapTask, int numReduceTask) {
      this.id = id;
      this.name = name;

      this.submitTick = submitTime*10000;
      this.inputSize = inputBytes/1024.0;
      this.shuffleSize = shuffleBytes/1024.0;
      this.outputSize = outputBytes/1024.0;
      this.blockSize = 128*1024.0;

      this.numMapTask = numMapTask;
      this.numReduceTask = numReduceTask;

      this.stage = 0;
    }

    public String toString() {
      if(numReduceTask > 0)
        return "j["+id+"](N"+name+" T"+submitTick+" M"+numMapTask+" r"+numReduceTask+" Mw0"+" Mo"+(int)(shuffleSize/numMapTask/1024)+" R"+numReduceTask+" Ri"+(int)(shuffleSize/numReduceTask/1024)+" Rw"+(int)(outputSize/numReduceTask/1024)+")";
      else
        return "j["+id+"](N"+name+" T"+submitTick+" M"+numMapTask+" r"+numReduceTask+" Mw"+(int)(outputSize/numMapTask/1024)+" Mo0 Ri0 Rw0)";
    }
  }

  private class JobList {
    int numJob;
    int numJobMax;
    HadoopJob[] jobs;

    JobList() {
      numJob = 0;
      numJobMax = 32768;
      jobs = new HadoopJob[numJobMax];
    }

    void jobAdd( String name, int submitTime, long inputBytes, long shuffleBytes, long outputBytes, int numMapTask, int numReduceTask) {
      jobs[numJob] = new HadoopJob( numJob, name, submitTime, inputBytes, shuffleBytes, outputBytes, numMapTask, numReduceTask);
      numJob++;
    }

    void importJobs() {
      try {
        File fileDir = new File("/home/jwu65/codes/temp/data/sample.csv");
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));
        String str;

        while((str = in.readLine()) != null) {
          String[] splits = str.split(",");
          jobAdd( splits[0], Integer.parseInt(splits[1]), Long.parseLong(splits[2]), Long.parseLong(splits[3]), Long.parseLong(splits[4]), Integer.parseInt(splits[5]), Integer.parseInt(splits[6]));
        }

        in.close();
      } catch(Exception e) {
        e.printStackTrace();
      }

      submitJobs();
    }

    void submitJobs() {
      for(int i=0; i<numJob; i++) {
        hTimeline.eventAdd( jobs[i].submitTick, 1, i, jobs[i].toString());
      }
    }
  }

  private class BaseEvent {

    int id;
    int tick;
    int type;// 0-time 0, 1-job arrived, 2-read finished, 3-write finished, 4-flow finished
    int value;
    String msg;
    Object obj;

    BaseEvent( int id, int tick, int type, Object obj) {
      this.id = id;
      this.tick = tick;
      this.type = type;
      this.obj = obj;
    }

    BaseEvent( int id, int tick, int type, int value, String msg) {
      this.id = id;
      this.tick = tick;
      this.type = type;
      this.value = value;
      this.msg = msg;
    }

    void print() {
      System.out.println("Tick: "+tick+" Event: "+msg);
    }

    void handle() {
      if(type == 1) {//job arrived: create tasks 
        HadoopJob j = (HadoopJob) obj;

        if(j.numReduceTask == 0){
          for(int i=0; i<j.numMapTask; i++){
            //Task t = new Task();
            //hTaskPool.add(t);
          }
        }
        else{
        }
      }

      if(type == 2) {//read finished: task.finishedFlow = 0, task.numFlow = numReplica, create write flow
      }

      if(type == 3) {//write finished: job.finishedTask++, free node
      }

      if(type == 4) {//flow finished: task.finishedFlow++, if task.finishedFlow < task.numFlow then create flow else create r/w finished
      }
    }

  }

  private class Timeline {

    int eId;
    int curTick;
    int end;
    //EventComparator eQueueCom;
    //PriorityQueue<BaseEvent> eQueue;
    LinkedList<BaseEvent> eQueue;
    LinkedList<BaseEvent> curEvents;

    Timeline() {
      this.curTick = 0;
      this.eId = 0;
      //this.eQueueCom = new EventComparator();
      //this.eQueue = new PriorityQueue<BaseEvent>( 100, this.eQueueCom);
      this.eQueue = new LinkedList<BaseEvent>();
      this.curEvents = new LinkedList<BaseEvent>();
      this.end = 24*3600*10000;//24hr
    }

    void processEvents() {
      while(!curEvents.isEmpty()) {
        handleEvent(curEvents.poll());
      }
      while(!eQueue.isEmpty() && eQueue.peek().tick <= curTick){
        handleEvent(eQueue.poll());
      }
    }

    void handleEvent(BaseEvent e) {

    }

    void scheduleTasks() {
      while(!hCluster.idlersIsEmpty() && !hTaskPool.isEmpty()) {
        hTaskPool.poll();
        //task.finishedFlow = 0, task.numFlow = nmap*nred, create read flows
      }
    }

    void nextTick() {
      curTick++;
    }

    void eventAdd(int tick, int type, int value, String msg) {
      BaseEvent e = new BaseEvent( eId++, tick, type, value, msg);
      eQueue.add(e);
    }

    void eventAdd( int type, int value) {
      BaseEvent e = new BaseEvent( eId++, curTick, type, value, null);
      curEvents.add(e);
    }

    void eventAdd( int type, Object obj) {
      BaseEvent e = new BaseEvent( eId++, curTick, type, obj);
      curEvents.add(e);
    }

    BaseEvent eventPoll() {
      return eQueue.poll();
    }

    void print() {
      while(!eQueue.isEmpty()) {
        BaseEvent e = eventPoll();
        e.print();
      }
    }

    private class EventComparator implements Comparator<BaseEvent> {
      @Override
      public int compare( BaseEvent e1, BaseEvent e2) {
        return e1.tick - e2.tick;
      }
    }

  }

  private class Task {
    int id;
    int jobId;
    int numSrc;
    int numDst;
    double inputSize;
    double outputSize;

    int numFinished;
    int[] srcNodeIds;
    int[] dstNodeIds;

    Task(int id, int jobId, int numSrc, int numDst, double inputSize, double outputSize) {
      this.id = id;
      this.jobId = jobId;
      this.numSrc = numSrc;
      this.numDst = numDst;
      this.inputSize = inputSize;
      this.outputSize = outputSize;

      numFinished = -1;
      srcNodeIds = new int[numSrc];
    }
  }

  private class TaskPool {
    LinkedList<Task> queuedTasks;
    LinkedList<Task> activeTasks;
    int tId;

    TaskPool() {
      queuedTasks = new LinkedList<Task>();
      activeTasks = new LinkedList<Task>();
      tId = 0;
    }

    void add(int jobId, int numSrc, int numDst, double inputSize, double outputSize) {
      Task t = new Task( tId++, jobId, numSrc, numDst, inputSize, outputSize);
      queuedTasks.add(t);
    }

    Task poll() {
      return queuedTasks.poll();
    }

    boolean isEmpty() {
      return queuedTasks.isEmpty();
    }

  }


  private class Cluster {
    
    int numCore = 0;
    int numTor = 0;
    int numNodePerTor = 0;

    double bwCT = 0;
    double bwTN = 0;

    Vertex[] cores = null;
    Vertex[] tors = null;
    Vertex[] nodes = null;

    LinkedList<Vertex> idlers;
    int fId;
    LinkedList<Flow> flows;
    LinkedList<Flow> flowsNext;
    Random randomGenerator;

    Cluster() {
      bwCT = 200;
      bwTN = 100;

      numCore = 2;
      numTor = 3;
      numNodePerTor = 4;

      createVertex();
      linkVertex();
      idlers = new LinkedList<Vertex>();

      fId = 0;
      flows = new LinkedList<Flow>();
      flowsNext = new LinkedList<Flow>();
      randomGenerator = new Random();
    }

    void createVertex() {
      cores = new Vertex[numCore];
      for(int i=0; i<numCore; i++) {
        cores[i] = new Vertex( 1, i);
      }

      tors = new Vertex[numTor];
      for(int i=0; i<numTor; i++) {
        tors[i] = new Vertex( 2, i);
      }

      nodes = new Vertex[numNodePerTor*numTor];
      for(int i=0; i<numNodePerTor*numTor; i++) {
        nodes[i] = new Vertex( 3, i);
      }
    }

    void linkVertex() {
      for(int i=0; i<numCore; i++) {
        Vertex core = cores[i];
        for(int j=0; j<numTor; j++) {
          core.connectWith( tors[j], bwCT);
        }
      }

      for(int i=0; i<numTor; i++){
        Vertex tor = tors[i];
        for(int j=0; j<numNodePerTor; j++) {
          Vertex node = nodes[i*numNodePerTor+j];
          tor.connectWith( node, bwTN);
        }
      }
    }

    boolean idlersIsEmpty(){
      return idlers.isEmpty();
    }

    void setAllIdle() {
      for(int i=0; i<numNodePerTor*numTor; i++) {
        idlers.add(nodes[i]);
      }
    }

    void addIdler(int i) {
      idlers.add(nodes[i]);
    }

    int pollIdler() {
      Vertex v = idlers.poll();
      if(v == null)
        return -1;
      else
        return v.id;
    }

    void addFlow( int srcId, int dstId, double dataSize, Task task) {
      Flow f = new Flow( fId++, srcId, dstId, dataSize, task);
      flowsNext.add(f);
    }

    void swapFlows() {
      LinkedList<Flow> temp = flows;
      flows = flowsNext;
      flowsNext = temp;
      flowsNext.clear();
    }

    void processFlows() {
      swapFlows();
      for(Flow f : flows) {
        if(f.finishedSize >= f.dataSize){
          for(Edge e : f.edgeRoute) {
            e.bw += f.bw;
          }
          hTimeline.eventAdd( 4, f.task);
        }
        else{
          double delta = 0.1;
          for(Edge e : f.edgeRoute) {
            if(e.bw + delta >= e.bwMax){
              delta = -f.bw/2;
              break;
            }
          }

          f.bw += delta;
          for(Edge e : f.edgeRoute) {
            e.bw += delta;
          }

          f.finishedSize += f.bw;
        }
      }
    }

    void print() {
      //printVerts( cores);
      printVerts( tors);
      //printVerts( nodes);
    }

    void printVerts( Vertex[] verts) {

      for(int i=0; i<verts.length; i++) {
        Vertex v = verts[i];

        System.out.print(v.type+"["+v.id+"] outEdges: ");
        for(Edge e : v.outEdges){
          System.out.print(e.dstVert.type+"["+e.dstVert.id+"]-"+e.bw+"/"+e.bwMax+"  ");
        }

        System.out.print("\n      inEdges: ");
        for(Edge e : v.inEdges){
          System.out.print(e.srcVert.type+"["+e.srcVert.id+"]-"+e.bw+"/"+e.bwMax+"  ");
        }

        System.out.print("\n...................\n");
      }
    }

    private class Flow {
      int id;
      Task task;
      Vertex src;
      Vertex dst;

      double dataSize;
      double finishedSize;
      double bw; // KB/step , KB/100ns, x10MB/s 
      //double srcBwMax;
      //double dstBwMax;

      LinkedList<Edge> edgeRoute;


      Flow( int id, int srcId, int dstId, double dataSize, Task task) {
        this.id = id;
        this.task = task;
        this.src = nodes[srcId];
        this.dst = nodes[dstId];
        this.dataSize = dataSize;
        this.finishedSize = 0;
        this.bw = 0.1;//init 1MB/s
        this.edgeRoute = new LinkedList<Edge>();

        createRoute();
      }

      void createRoute() {
        int srcTorId = src.id/numNodePerTor;
        int dstTorId = dst.id/numNodePerTor;

        edgeRoute.add(src.getEdgeTo(tors[srcTorId]));

        if(srcTorId != dstTorId){
          int coreId = randomGenerator.nextInt(numCore);
          edgeRoute.add(tors[srcTorId].getEdgeTo(cores[coreId]));
          edgeRoute.add(cores[coreId].getEdgeTo(tors[dstTorId]));  
        }

        edgeRoute.add(tors[dstTorId].getEdgeTo(dst));
      }
    }

    private class Vertex {

      int type;// 1core, 2tor, 3node, 4slot
      int id;

      LinkedList<Edge> outEdges;
      LinkedList<Edge> inEdges;

      //LinkedList<Flow> flows;

      Vertex(int type, int id) {
        this.type = type;
        this.id = id;
        this.outEdges = new LinkedList<Edge>();
        this.inEdges = new LinkedList<Edge>();
        //this.flows = new LinkedList<Flow>();
      }

      void connectWith( Vertex v, double bw) {
        Edge out = new Edge( this, v, bw);
        Edge in = new Edge( v, this, bw);

        this.outEdges.add(out);
        this.inEdges.add(in);
        v.outEdges.add(in);
        v.inEdges.add(out);
      }

      Edge getEdgeTo(Vertex v) {
        if(!outEdges.isEmpty()){
          for(Edge e : outEdges) {
            if(e.dstVert == v)
              return e;
          }
        }
        return null;
      }

    }

    private class Edge {

      Vertex srcVert;
      Vertex dstVert;

      double bw;
      double bwMax;

      //LinkedList<Flow> flows;

      Edge( Vertex srcVert, Vertex dstVert, double bwMax) {
        this.srcVert = srcVert;
        this.dstVert = dstVert;
        this.bwMax = bwMax;
        this.bw = 0;
        //this.flows = new LinkedList<Flow>();
      }
    }
    //Cluster End Here
  }

}

