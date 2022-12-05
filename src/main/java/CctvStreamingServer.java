import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class CctvStreamingServer {
    public static void main(String[] args) throws IOException{
        System.out.println("Starting CCTV Server main");
        ServerSocket listener = new ServerSocket(9090);
        BufferedReader reader = null;

        try{
            System.out.println("Trying to accept connection..");
            Socket socket = listener.accept();
            System.out.println("Accepted connection " + socket.toString());
            reader = new BufferedReader(new FileReader("/home/dani/Desktop/distributed-systems/flink-final-project/Input/Cctv-Detections.txt"));
            try{
                PrintWriter writer = new PrintWriter(socket.getOutputStream(),true);
                String readLine;

                while((readLine = reader.readLine()) != null){
                    writer.println(readLine);
                    Thread.sleep(500);
                }
            }finally {
                {
                    socket.close();
                }
            }
        } catch (Exception e){
                e.printStackTrace();
        }
        finally {
            System.out.println("Closing listener");
            listener.close();
            if(reader != null)
                reader.close();
        }
    }
}
