public class FamilyMember {
    public String idFamilyMember;
    public String fullName;
    public int age;
    public String gender;
    public String phone;

    public FamilyMember(String src) {
        String[] data = src.split(",");
        idFamilyMember = data[0];
        fullName = data[1];
        age = Integer.parseInt(data[2] == null? "0" : data[2]);
        gender = data[3];
        phone = data[4];
    }
}