package org.pdcl.hcsim;

import java.util.Random;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.io.*;

public class HCSim {

  public static void main(String[] args) throws Exception {

    HCSim hsim = new HCSim();
    hsim.run(args);

    return;
  }

  //global var
  SimHadoop hadoop;
  SimCluster cluster;
  SimEvents events;
  //global var end


  HCSim() throws Exception {
    events = new SimEvents();
    hadoop = new SimHadoop();
    cluster = new SimCluster();
  }

  void run(String[] args) throws Exception {
   
    cluster.setAllIdle();
    cluster.print();

    hadoop.importJobs();

    events.print();

    System.out.println("\n\n");
  }

  private class SimHadoop {

    static final int DFS_REPLICATION = 3;
  
    HadoopJobs jobs;
    HadoopTasks tasks;

    SimHadoop() {
      tasks = new HadoopTasks();
      jobs = new HadoopJobs();
    }

    void importJobs() {
      jobs.importJobs();
    }

    void scheduleTasks() {
      tasks.schedule();
    }

    void addTask(Task t) {
      tasks.add(t);
    }

    void finishTask(Task t) {
      tasks.finish(t);
    }
    
    private class HadoopJobs {
      int numJob;
      int numJobMax;
      Job[] jobs;

      HadoopJobs() {
        numJob = 0;
        numJobMax = 32768;
        jobs = new Job[numJobMax];
      }

      void jobAdd( String name, int submitTime, long inputBytes, long shuffleBytes, long outputBytes, int numMapTask, int numReduceTask) {
        jobs[numJob] = new Job( numJob, name, submitTime, inputBytes, shuffleBytes, outputBytes, numMapTask, numReduceTask);
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
          JobEvent e = new JobEvent(jobs[i]);
          events.addEvent(e);
        }
      }

    }

    private class JobEvent extends BaseEvent {
      Job j;

      JobEvent(Job j) {
        super(j.submitTick, 1, j.toString());
        this.j = j;
      }

      @Override
      void handle() {
        int numTask, numSrc;
        if(j.numReduceTask > 0){
          numTask = j.numReduceTask;
          numSrc = j.numMapTask;
        }
        else {
          numTask = j.numMapTask;
          numSrc = 0;
        }

        double inputSize = j.shuffleSize/numTask;
        double outputSize = j.outputSize/numTask;
        for(int i=0; i<numTask; i++) {
          Task t = new Task( j, numSrc, DFS_REPLICATION, inputSize, outputSize);
          hadoop.addTask(t);
        }
      }
    }
    
    private class HadoopTasks {
      LinkedList<Task> queuedTasks;
      LinkedList<Task> activeTasks;

      int utid;//unique task id

      HadoopTasks() {
        queuedTasks = new LinkedList<Task>();
        activeTasks = new LinkedList<Task>();
        utid = 0;
      }

      void add(Task t) {
        t.setId(utid++);
        queuedTasks.add(t);
      }

      Task poll() {
        return queuedTasks.poll();
      }

      void schedule() {
        while(!cluster.idlersIsEmpty() && !queuedTasks.isEmpty()) {
          Task t = queuedTasks.poll();
          t.unit = cluster.getIdler();

          activeTasks.add(t);
          cluster.runTask(t);
        }
      }

      void finish(Task t) {
        activeTasks.remove(t);
      }

    }

    private class TaskEvent extends BaseEvent {
      Task t;

      TaskEvent(Task t) {
        super(5);
        this.t = t;
      }

      @Override
      void handle() {
        if(t.status == 0){
          t.status = 1;
          cluster.runTask(t);
        }
        else{
          hadoop.finishTask(t);
          cluster.setIdle(t.unit);
        }
      }
    }
  }

  private class Job {
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

    Job( int id, String name, int submitTime, long inputBytes, long shuffleBytes, long outputBytes, int numMapTask, int numReduceTask) {
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
  
  private class Task {
    int id;
    Job job;
    Vertex unit;
    int numSrc;
    int numDst;
    double inputSize;
    double outputSize;

    int numFinished;
    //int[] srcNodeIds;
    //int[] dstNodeIds;

    int status;//0-reading, 1-writing

    Task(Job job, int numSrc, int numDst, double inputSize, double outputSize) {
      this.id = -1;
      this.job = job;
      this.numSrc = numSrc;
      this.numDst = numDst;
      this.inputSize = inputSize;
      this.outputSize = outputSize;
      this.unit = null;

      if(numSrc > 0)
        this.status = 0;
      else
        this.status = 1;
    }

    void setId(int id) {
      this.id = id;
    }

    public String toString() {
      return "this is a task";
    }
  }

  private class BaseEvent {

    int id;
    int tick;
    int type;// 0-time 0, 1-job arrived, 2-read finished, 3-write finished, 4-flow finished
    String msg;

    BaseEvent( int type) {
      this.id = -1;
      this.tick = -1;
      this.type = type;
    }

    BaseEvent( int tick, int type) {
      this.id = -1;
      this.tick = tick;
      this.type = type;
    }

    BaseEvent( int tick, int type, String msg) {
      this.id = -1;
      this.tick = tick;
      this.type = type;
      this.msg = msg;
    }

    void setId(int id) {
      this.id = id;
    }

    void print() {
      System.out.println("Tick: "+tick+" Event: "+msg);
    }

    void handle() {
      if(type == 2) {//read finished: task.finishedFlow = 0, task.numFlow = numReplica, create write flow
      }

      if(type == 3) {//write finished: job.finishedTask++, free node
      }

      if(type == 4) {//flow finished: task.finishedFlow++, if task.finishedFlow < task.numFlow then create flow else create r/w finished
      }
    }

  }



  private class SimEvents {

    int eId;
    int curTick;
    int end;
    //EventComparator queuedEventListCom;
    //PriorityQueue<BaseEvent> queuedEventList;
    LinkedList<BaseEvent> queuedEventList;
    LinkedList<BaseEvent> currentEventList;

    SimEvents() {
      this.curTick = 0;
      this.eId = 0;
      //this.queuedEventListCom = new EventComparator();
      //this.queuedEventList = new PriorityQueue<BaseEvent>( 100, this.queuedEventListCom);
      this.queuedEventList = new LinkedList<BaseEvent>();
      this.currentEventList = new LinkedList<BaseEvent>();
      this.end = 24*3600*10000;//24hr
    }

    void process() {
      while(!currentEventList.isEmpty()) {
        currentEventList.poll().handle();
      }
      while(!queuedEventList.isEmpty() && queuedEventList.peek().tick <= curTick){
        queuedEventList.poll().handle();
      }
    }

    void nextTick() {
      curTick++;
    }

    void addEvent(BaseEvent e) {
      e.setId(eId++);
      if(e.tick > curTick)
        queuedEventList.add(e);
      else
        currentEventList.add(e);
    }

    BaseEvent eventPoll() {
      return queuedEventList.poll();
    }

    void print() {
      while(!queuedEventList.isEmpty()) {
        BaseEvent e = eventPoll();
        e.print();
      }
    }

    private class BaseEventComparator implements Comparator<BaseEvent> {
      @Override
      public int compare( BaseEvent e1, BaseEvent e2) {
        return e1.tick - e2.tick;
      }
    }

  }



  private class SimCluster {
    
    int numCore;
    int numTor;
    int numNodePerTor;

    double bwCT;
    double bwTN;

    Vertex[] cores;
    Vertex[] tors;
    Vertex[] nodes;

    int numUnit;
    Vertex[] units;

    LinkedList<Vertex> idlers;
    int fId;
    LinkedList<Flow> flows;
    LinkedList<Flow> flowsNext;
    Random randomGenerator;

    SimCluster() {
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

      numUnit = numNodePerTor*numTor;
      units = nodes;
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

    void setIdle(Vertex v) {
      idlers.add(v);
    }

    Vertex getIdler() {
      return idlers.poll();
    }

    void setAllIdle() {
      for(int i=0; i<numUnit; i++) {
        idlers.add(units[i]);
      }
    }

    void addIdler(Vertex v) {
      idlers.add(v);
    }

    void runTask(Task t) {
      if(t.status == 0) {
        double dataSize = t.inputSize/t.numSrc;
        for(int i=0; i<t.numSrc; i++) {
          Vertex u = units[randomGenerator.nextInt(numUnit)];
          Flow f = new Flow( t, u, t.unit, dataSize);
          addFlow(f);
        }
      }
      else {
        double dataSize = t.outputSize/t.numDst;
        for(int i=0; i<t.numDst; i++) {
          Vertex u = units[randomGenerator.nextInt(numUnit)];
          Flow f = new Flow( t, t.unit, u, dataSize);
          addFlow(f);
        }
      }
    }

    void addFlow(Flow f) {
      f.setId(fId++);
      flowsNext.add(f);
    }

    void swapFlows() {
      LinkedList<Flow> temp = flows;
      flows = flowsNext;
      flowsNext = temp;
      flowsNext.clear();
    }

    void process() {
      swapFlows();
      for(Flow f : flows) {
        if(f.finishedSize >= f.dataSize){
          for(Edge e : f.edgeRoute) {
            e.bw += f.bw;
          }
          //events.addEvent( 4, f.taskId);
        }
        else {
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


      Flow( Task task, Vertex src, Vertex dst, double dataSize) {
        this.id = -1;
        this.task = task;
        this.src = src;
        this.dst = dst;
        this.dataSize = dataSize;
        this.finishedSize = 0;
        this.bw = 0.1;//init 1MB/s
        this.edgeRoute = new LinkedList<Edge>();

        createRoute();
      }

      void setId(int id) {
        this.id = id;
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
  }//SimCluster End Here

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
  

}
