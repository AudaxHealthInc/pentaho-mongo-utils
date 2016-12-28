/*!
  * Copyright 2010 - 2015 Pentaho Corporation.  All rights reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */
package org.pentaho.mongo.wrapper;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of MongoClientWrapper which uses no credentials.
 * Should only be instantiated by MongoClientWrapperFactory.
 */
class UsernamePasswordMongoClientWrapper extends NoAuthMongoClientWrapper {
  private final String user;

  /**
   * Create a connection to a Mongo server based on parameters supplied in the step meta data
   *
   * @param props properties to use
   * @param log   for logging
   * @throws MongoDbException if a problem occurs
   */
  UsernamePasswordMongoClientWrapper( MongoProperties props, MongoUtilLogger log )
    throws MongoDbException {
    super( props, log );
    user = props.get( MongoProp.USERNAME );
  }

  UsernamePasswordMongoClientWrapper( MongoClient mongo, MongoUtilLogger log, String user ) {
    super( mongo, null, log );
    props = null;
    this.user = user;
  }

  public String getUser() {
    return user;
  }

  /**
   * Create a credentials object
   *
   * Supports special notation for username field, if it has a '@' symbol, prefix will be considered a
   * username, suffix is an account's source database. Through this mechanism accounts that are created in
   * Mongo's 'admin' database and granted permissions to other databases are supported.
   *
   * @return a configured MongoCredential object
   */
  @Override
  public List<MongoCredential> getCredentialList() {
    List<MongoCredential> credList = new ArrayList<MongoCredential>();

    String database = props.get(MongoProp.DBNAME);

    String username;

    String principal = props.get(MongoProp.USERNAME);
    String[] principalSegments = principal.split("@");
    if (principalSegments.length == 1) {
      if (database == null) {
        database = "admin";
      }
      username = principal;
    } else if (principalSegments.length == 2) {
      username = principalSegments[0];
      database = principalSegments[1];
    } else {
      throw new IllegalArgumentException("Illegal username '" + principal + "' it should either have one '@' or none");
    }

    credList.add( MongoCredential.createCredential(
        username,
        database,
        props.get( MongoProp.PASSWORD ).toCharArray() ) );
    return credList;
  }
}
