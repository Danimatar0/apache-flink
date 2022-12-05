public class SuspiciousVisitor {
    public String idSuspicious;
    public String fullName;
    public int age;
    public String gender;

    public SuspiciousVisitor(String src) {
        String[] data = src.split(",");
        idSuspicious = data[0];
        fullName = data[1];
        age = Integer.parseInt(data[2] == null? "0" : data[2]);
        gender = data[3];
    }
}
