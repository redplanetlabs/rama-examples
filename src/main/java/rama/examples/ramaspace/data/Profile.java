package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class Profile implements RamaSerializable {
  public String email;
  public String displayName;
  public String bio;
  public String location;
  public String profilePic;
  public long joinedAtMillis;

  public Profile(String email, String displayName, String bio, String location, String profilePic, long joinedAtMillis) {
    this.email = email;
    this.displayName = displayName;
    this.bio = bio;
    this.location = location;
    this.profilePic = profilePic;
    this.joinedAtMillis = joinedAtMillis;
  }
}
