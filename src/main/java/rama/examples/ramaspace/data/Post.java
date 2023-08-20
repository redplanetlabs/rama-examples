package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class Post implements RamaSerializable {
  public String userId;
  public String toUserId;
  public String content;

  public Post(String userId, String toUserId, String content) {
    this.userId = userId;
    this.toUserId = toUserId;
    this.content = content;
  }
}