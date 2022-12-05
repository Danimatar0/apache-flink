public class DetectedThief {
    public String idThief;
    public String fullName;
    public int age;
    public String gender;

    public DetectedThief(String src) {
        String[] data = src.split(",");
        idThief = data[0];
        fullName = data[1];
        age = Integer.parseInt(data[2] == null? "0" : data[2]);
        gender = data[3];
    }
}
