package com.amazon.redshift;

import com.amazon.redshift.plugin.utils.RequestUtils;

import java.util.Date;


public class NativeTokenHolder {
  protected String m_accessToken;
  private Date m_expiration;
  private boolean refresh; // true means newly added, false means from cache.

  protected NativeTokenHolder(String accessToken)
  {
      this(accessToken, new Date(System.currentTimeMillis() + 15 * 60 * 1000));
  }

  protected NativeTokenHolder(String accessToken, Date expiration)
  {
      this.m_accessToken = accessToken;
      this.m_expiration = expiration;
  }
  
  public static NativeTokenHolder newInstance(String accessToken)
  {
      return new NativeTokenHolder(accessToken);
  }

  public static NativeTokenHolder newInstance(String accessToken, Date expiration)
  {
      return new NativeTokenHolder(accessToken, expiration);
  }
  
  public boolean isExpired()
  {
      return RequestUtils.isCredentialExpired(m_expiration);
  }

  public String getAccessToken()
  {
    return m_accessToken;
  }
  
  public Date getExpiration()
  {
      return m_expiration;
  }
  
  public void setRefresh(boolean flag) 
  {
    refresh = flag;
  }

  public boolean isRefresh() 
  {
    return refresh;
  }
}
