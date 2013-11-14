Java MTProto Implementation
===========

[Mobile Transport protocol](http://core.telegram.org/mtproto) used in [Telegram](http://telegram.org/) project and designed for speed and security over weird mobile networks.

This is library of [MTProto](http://core.telegram.org/mtproto) implementation for java. Now it used in our production-ready product [Telegram S](https://play.google.com/store/apps/details?id=org.telegram.android).

[![MTProto build server](http://ci.81port.com/app/rest/builds/buildType:%28id:TelegramNetworking_JavaMtProto%29/statusIcon)](http://ci.81port.com/viewType.html?buildTypeId=TelegramNetworking_JavaMtProto)

Including in your project
-----------

#### Dependencies

This project depends only on [tl-core library](https://github.com/ex3ndr/telegram-tl-core)

#### Binary usage

You may download distribution at [releases page](https://github.com/ex3ndr/telegram-mt/releases) and include it to your project.

#### Compilation from sources

1. Checkout this repo to ````mtproto```` folder
2. Checkout [tl-core repo](https://github.com/ex3ndr/telegram-tl-core) to ````tl-core```` folder
3. Execute ````gradle build```` from ````mtproto```` folder

More information
----------------
#### MTProto documentation

English: http://core.telegram.org/mtproto

Russian: http://dev.stel.com/mtproto

#### Type Language documentation

English: http://core.telegram.org/mtproto/TL

Russian: http://dev.stel.com/mtproto/TL

####Telegram project

http://telegram.org/

#### Android Client that uses this library

[![Telegram S](https://developer.android.com/images/brand/en_generic_rgb_wo_45.png)](https://play.google.com/store/apps/details?id=org.telegram.android "Telegram S")

Licence
----------------
Compiler uses [MIT Licence](LICENCE)
