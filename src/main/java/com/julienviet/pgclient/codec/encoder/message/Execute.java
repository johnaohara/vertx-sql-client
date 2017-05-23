package com.julienviet.pgclient.codec.encoder.message;

import com.julienviet.pgclient.codec.decoder.message.ReadyForQuery;
import com.julienviet.pgclient.codec.Message;
import com.julienviet.pgclient.codec.decoder.message.CommandComplete;
import com.julienviet.pgclient.codec.decoder.message.EmptyQueryResponse;
import com.julienviet.pgclient.codec.decoder.message.ErrorResponse;
import com.julienviet.pgclient.codec.decoder.message.PortalSuspended;
import com.julienviet.pgclient.codec.decoder.message.RowDescription;

import java.util.Objects;

/**
 *
 * <p>
 * The message specifies the portal and a maximum row count (zero meaning "fetch all rows") of the result.
 *
 * <p>
 * The row count of the result is only meaningful for portals containing commands that return row sets;
 * in other cases the command is always executed to completion, and the row count of the result is ignored.
 *
 * <p>
 * The possible responses to this message are the same as {@link Query} message, except that
 * it doesn't cause {@link ReadyForQuery} or {@link RowDescription} to be issued.
 *
 * <p>
 * If Execute terminates before completing the execution of a portal, it will send a {@link PortalSuspended} message;
 * the appearance of this message tells the frontend that another Execute should be issued against the same portal to
 * complete the operation. The {@link CommandComplete} message indicating completion of the source SQL command
 * is not sent until the portal's execution is completed. Therefore, This message is always terminated by
 * the appearance of exactly one of these messages: {@link CommandComplete},
 * {@link EmptyQueryResponse}, {@link ErrorResponse} or {@link PortalSuspended}.
 *
 * @author <a href="mailto:emad.albloushi@gmail.com">Emad Alblueshi</a>
 */

public class Execute implements Message {

  private String portal;
  private int rowCount;


  public String getPortal() {
    return portal;
  }

  public int getRowCount() {
    return rowCount;
  }

  public Execute setPortal(String portal) {
    this.portal = portal;
    return this;
  }

  public Execute setRowCount(int rowCount) {
    this.rowCount = rowCount;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Execute execute = (Execute) o;
    return rowCount == execute.rowCount &&
      Objects.equals(portal, execute.portal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(portal, rowCount);
  }


  @Override
  public String toString() {
    return "Execute{" +
      ", portal='" + portal + '\'' +
      ", rowCount=" + rowCount +
      '}';
  }

}