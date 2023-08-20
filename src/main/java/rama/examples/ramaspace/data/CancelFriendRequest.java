package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class CancelFriendRequest implements RamaSerializable {
  public String userId;
  public String toUserId;

  public CancelFriendRequest(String userId, String toUserId) {
    this.userId = userId;
    this.toUserId = toUserId;
  }
}