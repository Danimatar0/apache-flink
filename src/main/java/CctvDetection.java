public class CctvDetection {
    public String idDetection;
    public String fullName;
    public int age;
    public String gender;
    public String reasonOfVisit;
    public String dateOfDetection;

    public CctvDetection(String src) {
        String[] data = src.split(",");
        idDetection = data[0];
        fullName = data[1];
        age = Integer.parseInt(data[2] == null? "0" : data[2]);
        gender = data[3];
        reasonOfVisit = data[4];
        dateOfDetection = data[5];
    }
}
