package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class UserRegistration implements RamaSerializable {
  public String userId;
  public String email;
  public String displayName;
  public int pwdHash;
  public String registrationUUID;

  public UserRegistration(String userId, String email, String displayName, int pwdHash, String registrationUUID) {
    this.userId = userId;
    this.email = email;
    this.displayName = displayName;
    this.pwdHash = pwdHash;
    this.registrationUUID = registrationUUID;
  }
}
