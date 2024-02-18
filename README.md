<h1>Version list Maven plugin</h1><br>

Maven plugin for retrieving the version list of an artifact

[![][Build Status img]][Build Status]
[![][Coverage Status img]][Coverage Status]
[![][Dependency Status img]][Dependency Status]
[![][License img]][License]
[![][Maven Central img]][Maven Central]
[![][Javadocs img]][Javadocs]

Plugin documentation can be found at https://lucapino.github.io/versions-list-maven-plugin

*Maven 3.3.&ast; artifact*
```
<plugin>
     <groupId>com.github.lucapino</groupId>
     <artifactId>versions-list-maven-plugin</artifactId>
     <version>1.0.1</version>
</plugin>
```

*Maven 3.5.&ast; artifact*
```
<plugin>
     <groupId>com.github.lucapino</groupId>
     <artifactId>versions-list-maven-plugin</artifactId>
     <version>2.1.0</version>
</plugin>
```

Available goals:
---
* **list** - create a project property containing the versions list of an artifact

Example plugin definition:
==========================
*Maven 3.3.&ast; artifact*
```
<plugin>
   <groupId>com.github.lucapino</groupId>
   <artifactId>versions-list-maven-plugin</artifactId>
   <version>1.0.1</version>
   <configuration>
       <groupId>commons-collections</groupId>
       <artifactId>commons-collections</artifactId>
       <startingVersion>1.0</startingVersion>
       <versionListPropertyName>versionList</versionListPropertyName>
   </configuration>
</plugin>
```

*Maven 3.5.&ast; artifact*
```
<plugin>
   <groupId>com.github.lucapino</groupId>
   <artifactId>versions-list-maven-plugin</artifactId>
   <version>2.0.0</version>
   <configuration>
       <groupId>commons-collections</groupId>
       <artifactId>commons-collections</artifactId>
       <startingVersion>1.0</startingVersion>
       <versionListPropertyName>versionList</versionListPropertyName>
   </configuration>
</plugin>
```
    
Example plugin definition:
==========================
```
<configuration>
   <groupId>commons-collections</groupId>
   <artifactId>commons-collections</artifactId>
   <startingVersion>1.0</startingVersion>
   <versionListPropertyName>versionList</versionListPropertyName>
</configuration>
```

[Build Status]:https://travis-ci.org/lucapino/versions-list-maven-plugin
[Build Status img]:https://travis-ci.org/lucapino/versions-list-maven-plugin.svg?branch=master

[Coverage Status]:https://codecov.io/gh/lucapino/versions-list-maven-plugin
[Coverage Status img]:https://codecov.io/gh/lucapino/versions-list-maven-plugin/branch/master/graph/badge.svg

[Dependency Status]:https://snyk.io/test/github/lucapino/versions-list-maven-plugin
[Dependency Status img]:https://snyk.io/test/github/lucapino/versions-list-maven-plugin/badge.svg?style=flat

[License]:LICENSE
[License img]:https://img.shields.io/badge/license-Apache%202-blue.svg

[Maven Central]:https://maven-badges.herokuapp.com/maven-central/com.github.lucapino/versions-list-maven-plugin
[Maven Central img]:https://maven-badges.herokuapp.com/maven-central/com.github.lucapino/versions-list-maven-plugin/badge.svg

[Javadocs]:http://www.javadoc.io/doc/com.github.lucapino/versions-list-maven-plugin
[Javadocs img]:http://javadoc.io/badge/com.github.lucapino/versions-list-maven-plugin.svg
