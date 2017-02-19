# HDP Test Examples

The goal of this project is to create unit test examples for common
HDP libraries. We will be focusing on Pig, Spark, HBase, Hive, and
Hadoop. It is the goal of this project to supply tests in Scala and 
Java.

## Running the Tests

Below is an example of how to run the current test suite using a
given profile. The profile hdp-2.4.2 is used in the example below. 

```bash
$ mvn clean test -P hdp-2.4.2
```


## Testing Libraries that Are Used

1. [JUnit](http://junit.org/junit4/)
2. [Mockito](http://mockito.org/)
3. [ScalaTest](http://www.scalatest.org/)
4. [Spark Testing Base](https://spark-packages.org/package/holdenk/spark-testing-base). Its source can be found [here](https://github.com/holdenk/spark-testing-base).
5. [PigUnit](http://pig.apache.org/docs/r0.15.0/test.html#pigunit). This link points to version 0.15.0 of pigunit.


## Test Data

Using sensor data from this [tutorial](http://hortonworks.com/hadoop-tutorial/how-to-analyze-machine-and-sensor-data/)

## Setting up Kerberos Enabled Cluster

### Creating cluster (Summarizing this [tutorial](https://cwiki.apache.org/confluence/display/AMBARI/Quick+Start+Guide))

1. Clone Ambari Vagrant Repository and Create the 3 Nodes
```
git clone https://github.com/u39kun/ambari-vagrant.git
cd ambari-vagrant/centos6.4
./up.sh 3
```

2. Once this is Complete
```
vagrant ssh c6401
sudo su -
wget -O /etc/yum.repos.d/ambari.repo http://public-repo-1.hortonworks.com/ambari/centos6/2.x/updates/2.2.1.0/ambari.repo
yum install ambari-server -y
ambari-server setup -s
ambari-server start
```

3. Setting up the Cluster via Ambari:
    * Go to: sandbox.hortonworks.com:8080
    * Login using: 
        - username: admin
        - password: admin
    * Click 'Launch Install Wizard'
    * Type in a name that you want to call this cluster. Hit next.
    * Choose HDP version. This is based on HDP 2.4. Click Next
    * In host names add: c64[01-03].ambari.apache.org
    * Select the file named 'insecure_private_key'
    * SSH User account should be 'root'
    * Click 'Register and Confirm'. The 3 nodes you created should now be listed.
    * Click OK
    * Click Next once host confirmation is successful.
    * Continue through the wizard. Install components you'd like to use.
    * Once cluster is setup continue to the next step to enable kerberos.
    
### Setting Up Kerberos

1. Install necessary Kerberos tools and create database.

```
yum install krb5-server krb5-libs krb5-auth-dialog rng-tools -y
rngd -r /dev/urandom -o /dev/random
/usr/sbin/kdb5_util create -s
```

2. Update '/etc/krb5.conf'. Example below.

```
[libdefaults]
  renew_lifetime = 7d
  forwardable = true
  default_realm = HORTONWORKS.LOCAL
  ticket_lifetime = 24h
  dns_lookup_realm = false
  dns_lookup_kdc = false
  #default_tgs_enctypes = aes des3-cbc-sha1 rc4 des-cbc-md5
  #default_tkt_enctypes = aes des3-cbc-sha1 rc4 des-cbc-md5


[domain_realm]

  hortonworks.local = HORTONWORKS.LOCAL

  .hortonworks.local = HORTONWORKS.LOCAL



[logging]
  default = FILE:/var/log/krb5kdc.log
  admin_server = FILE:/var/log/kadmind.log
  kdc = FILE:/var/log/krb5kdc.log

[realms]
  HORTONWORKS.LOCAL = {
    admin_server = sandbox.hortonworks.com
    kdc = sandbox.hortonworks.com
  }


```

3. Restart Services

```
/etc/rc.d/init.d/krb5kdc restart
/etc/rc.d/init.d/kadmin restart
```

4. Adding admin principal
```
sudo kadmin.local
kadmin.local:  add_principal admin/admin@EXAMPLE.COM
WARNING: no policy specified for admin/admin@EXAMPLE.COM; defaulting to no policy
Enter password for principal "admin/admin@EXAMPLE.COM":
Re-enter password for principal "admin/admin@EXAMPLE.COM":
Principal "admin/admin@EXAMPLE.COM" created.
```

### Enabling Kerberos in Ambari

1. 

### Creating Keytab

```
kadmin
addprinc jj@EXAMPLE.COM
ktutil
addent -password -p jj -k 1 -e RC4-HMAC
wkt jj.keytab
q
```

### Retrieving Necessary Files

1. Replace the following files with files obtained from the cluster just created:
    * src/main/resources/core-site.xml
    * src/main/resources/hbase-site.xml
    * src/main/resources/hdfs-site.xml
    * src/main/resources/jj.keytab
    * src/main/resources/krb5.conf
    
    -javaagent:/Users/ssrisunt/Downloads/spring-aop-blog-master/spring-aop-aspectj-ltw/aspectjweaver-1.8.8.jar
    -noverify
    
    # Date Range

## Past 12 Calendar Months

> select date_sub(concat(from_unixtime(unix_timestamp('2017-02-06 20:00:00'), 'yyyy-MM'), '-01'),1) as END_DATE , > DATE_SUB(date_sub(concat(from_unixtime(unix_timestamp('2017-02-06 20:00:00'), 'yyyy-MM'), '-01'),1), 12\*30) as  12_months_from_Date,
> DATE_ADD(DATE_SUB(date_sub(concat(from_unixtime(unix_timestamp('2017-02-06 20:00:00'), 'yyyy-MM'), '-01'),1),  12\*30),1-DAY(DATE_SUB(date_sub(concat(from_unixtime(unix_timestamp('2017-02-06 20:00:00'), 'yyyy-MM'), '-01'),1), 12\*30))) as START_DATE
