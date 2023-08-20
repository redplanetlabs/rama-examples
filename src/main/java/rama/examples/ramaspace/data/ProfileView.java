package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class ProfileView implements RamaSerializable {
  public String userId;
  public String toUserId;
  public long timestamp;

  public ProfileView(String userId, String toUserId, long timestamp) {
    this.userId = userId;
    this.toUserId = toUserId;
    this.timestamp = timestamp;
  }
}