package rama.examples.ramaspace.data;

import com.rpl.rama.RamaSerializable;

public class ProfileEdit implements RamaSerializable {
  public String userId;
  public String field;
  public Object value;

  public ProfileEdit(String userId, String field, Object value) {
    this.userId = userId;
    this.field = field;
    this.value = value;
  }
}
