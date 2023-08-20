package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class FriendRequest implements RamaSerializable {
  public String userId;
  public String toUserId;

  public FriendRequest(String userId, String toUserId) {
    this.userId = userId;
    this.toUserId = toUserId;
  }
}
