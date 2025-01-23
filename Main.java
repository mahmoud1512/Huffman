import java.io.*;
import java.util.*;



class Node implements Serializable
{
    long frequency;
    Node left;
    Node right;
    List<Byte>data;

    public Node() {
    }
    public Node(long frequency,List<Byte>data) {
        this.frequency = frequency;
        this.data=data;
    }
}

class Compression
{
    private final Map<List<Byte>,String>compressionMap=new HashMap<>();
    private final Map<List<Byte>,Long> byteMap =new HashMap<>();
    private final PriorityQueue<Node>nodeQueue=new PriorityQueue<>(Comparator.comparingLong(e -> e.frequency));
    private List<Node>PQasList;

    void buildMapOfSequences(Node root,String build)
    {
        if(root==null)
        {
            return;
        }
        if(root.left==null&&root.right==null) {
            compressionMap.put(root.data, build);
            return;
        }


        buildMapOfSequences(root.left,build+"0");
        buildMapOfSequences(root.right,build+"1");

    }

    void CalculateStats(int n,String path) throws IOException {

        File inputFile =new File(path);


        int BufferSize=n*1024;   //n KB
        byte[]bytes=new byte[BufferSize];
        BufferedInputStream bufferedInputStream=new BufferedInputStream(new FileInputStream(inputFile),BufferSize);
        int readBytes;
        while ((readBytes=bufferedInputStream.read(bytes))!=-1)
        {
            //Calculate Statistics
            int generalIndex=0;

            int lengthOfSequences =Math.ceilDiv(readBytes,n);
            for (int i = 0; i < lengthOfSequences; i++) {
                //Building a sequence
                List<Byte>byteList=new ArrayList<>(n);
                for (int j = 0; j < n; j++) {
                    if(generalIndex==readBytes)
                        break;
                    byteList.add(bytes[generalIndex]);
                    generalIndex++;
                }
                //check the sequence
                if (byteMap.containsKey(byteList))
                {
                    byteMap.put(byteList,byteMap.get(byteList)+1);
                }
                else
                {
                    byteMap.put(byteList,1L);
                }

            }
        }
        bufferedInputStream.close();


    }

    void buildTreeAndMap(String inPath,String outPath) throws IOException {

        for (List<Byte> list : byteMap.keySet()) {
            Node node=new Node(byteMap.get(list), list);
            nodeQueue.add(node);
        }

        int characters = byteMap.keySet().size();

        PQasList=new ArrayList<>(nodeQueue);
        writeHeader(inPath,outPath);

        if(characters==1)   //Special case
        {
            Node node=new Node();
            node.left=nodeQueue.peek();
            nodeQueue.add(node);
        }




        for (int i = 1; i <= characters - 1; i++) {
            Node node = new Node();
            Node x = nodeQueue.poll();
            Node y = nodeQueue.poll();
            node.left = x;
            node.right = y;
            node.frequency = x.frequency + y.frequency;
            nodeQueue.add(node);
        }
        buildMapOfSequences(nodeQueue.peek(), "");
    }

    void writeBodyToOutputFile(int n,String path,String outPath) throws IOException {
        File outputFile=new File(outPath);
        FileOutputStream outputStream=new FileOutputStream(outputFile,true);



        File file=new File(path);
        FileInputStream inputStream=new FileInputStream(file);
        byte[]bytes=new byte[n*1024];
        BufferedInputStream bufferedInputStream=new BufferedInputStream(inputStream,n*1024);
        StringBuilder compressByteAsAString= new StringBuilder("");

        BufferedOutputStream bufferedOutputStream=new BufferedOutputStream(outputStream);


        int readBytes;
        while ((readBytes=bufferedInputStream.read(bytes))!=-1) {

            int generalIndex = 0;
            int readLen=Math.ceilDiv(readBytes,n);
            for (int i = 0; i < readLen; i++) {
                //Building a sequence
                List<Byte> byteList = new ArrayList<>();
                for (int j = 0; j < n; j++) {
                    if (generalIndex == readBytes)
                        break;
                    byteList.add(bytes[generalIndex]);
                    generalIndex++;
                }
                //process the sequence
                String mappingOfList = compressionMap.get(byteList);

                compressByteAsAString.append(mappingOfList);
                while (compressByteAsAString.length() >= 8) {

                    String p1 = compressByteAsAString.substring(0, 8);
                    compressByteAsAString.delete(0,8);
                    int value = Integer.parseInt(p1, 2);
                    byte b = (byte) value;
                    bufferedOutputStream.write(b);

                }

            }
        }

        if (!compressByteAsAString.isEmpty()) {    //Handling last Byte
            int margin =8-compressByteAsAString.length();
            compressByteAsAString.append("0".repeat(Math.max(0, margin)));
            int value = Integer.parseInt(String.valueOf(compressByteAsAString), 2);
            byte b=(byte) value;
            bufferedOutputStream.write(b);

        }
        bufferedInputStream.close();
        bufferedOutputStream.close();    //To force any data in buffer to be written

    }

    void writeHeader(String inPath,String outPath) throws IOException {

        File file=new File(outPath);
        FileOutputStream fileOutputStream=new FileOutputStream(file);
        BufferedOutputStream bufferedOutputStream=new BufferedOutputStream(fileOutputStream);
        DataOutputStream dataOutputStream=new DataOutputStream(bufferedOutputStream);
        File inputFile=new File(inPath);

        long size=inputFile.length();
        dataOutputStream.writeLong(size);
        dataOutputStream.writeInt(PQasList.size());
        for (Node node:PQasList)
        {
            dataOutputStream.writeLong(node.frequency);
            if(node.data!=null) {
                dataOutputStream.writeInt(node.data.size());  //size of node Data
                for (Byte b:node.data)
                {
                    dataOutputStream.writeByte(b);
                }
            }
            else
            {
                dataOutputStream.writeInt(0);
            }

        }

        dataOutputStream.close();


    }

}



class Decompression
{

    BufferedOutputStream writer=null;
    private Node root=null;
    private long bytesInFile=0;
    List<Byte>generalData;


    void extractObjects(String compressedFile,String decompressedFile) throws IOException {
        //chunk
        byte[]bytes=new byte[32*1024];

        //Defining Streams
        File file=new File(compressedFile);
        FileInputStream fileInputStream=new FileInputStream(file);
        BufferedInputStream bufferedInputStream=new BufferedInputStream(fileInputStream);
        DataInputStream inputStream=new DataInputStream(bufferedInputStream);
        File extractedFile=new File(decompressedFile);
        writer=new BufferedOutputStream(new FileOutputStream(extractedFile));

        //reading Header

        bytesInFile=inputStream.readLong();



        int listSize=inputStream.readInt();
        PriorityQueue<Node>pq=new PriorityQueue<>(Comparator.comparingLong(e -> e.frequency));
        for (int i = 0; i < listSize; i++) {
             long frequency=inputStream.readLong();
             int dataSize=inputStream.readInt();
             List<Byte>byteList=new ArrayList<>(dataSize);
            for (int j = 0; j < dataSize; j++) {
                  byteList.add(inputStream.readByte());
            }
            pq.add(new Node(frequency,byteList));
        }
        int characters= pq.size();

        if (characters==1)
        {
            root=new Node();
            root.left=pq.poll();
        }

        for (int i = 1; i <= characters - 1; i++) {
                Node node = new Node();
                Node n1 = pq.poll();
                Node n2 = pq.poll();
                node.left = n1;
                node.right = n2;
                node.frequency = n1.frequency + n2.frequency;
                pq.add(node);
        }



        if (characters>1)
         root=pq.peek();


        //Loop variables
        Node current=root;
        int count=-1;


        while ((count=bufferedInputStream.read(bytes))!=-1)
        {

            for (int i=0;i<count;i++)
            {
                byte b=bytes[i];
                for (int j = 7; j >=0 ; j--) {
                    if (bytesInFile==0)
                        break;
                   boolean indicator=((b&(1<<j))==0);

                   if (indicator)
                       current=current.left;
                   else
                       current=current.right;

                   if(current.left==null&&current.right==null)
                   {
                       generalData=current.data;
                       bytesInFile-=generalData.size();
                       for (Byte byt:generalData) {
                           writer.write(byt);
                       }
                       current=root;
                   }
                }
            }
        }
        writer.close();
        inputStream.close();
    }


}








public class Main {

    public static void main(String[] args) throws IOException {


        if(args[0].equals("c"))
        {
            Compression compression=new Compression();

            //Compression
            long initial=System.currentTimeMillis();

            String path=args[1];
            int n= Integer.parseInt(args[2]);
            File file=new File(path);
            String outPath= file.getParent()+File.separator+"21011275"+"."+n+"."+file.getName()+"."+"hc";
            compression.CalculateStats(n,path);
            compression.buildTreeAndMap(path,outPath);
            compression.writeBodyToOutputFile(n,path,outPath);
            System.out.println("Time taken in compression "+((System.currentTimeMillis()-initial)/1000));

            File Cfile=new File(outPath);

            System.out.println("Compression ratio is "+((double)Cfile.length()/ file.length()));

        }

        else
        {
            //Decompression :

            Decompression decompression=new Decompression();
            String path=args[1];
            File file=new File(path);

            String name=file.getName();
            int hcPlace=name.lastIndexOf(".hc");
            name=name.substring(0,hcPlace);

            String exPath=file.getParent()+File.separator+"extracted."+name;


            long startD=System.currentTimeMillis();
            decompression.extractObjects(path,exPath);
            System.out.println("Time taken in decompression "+((System.currentTimeMillis()-startD)/1000));


        }








}




}