/**
 * NoiseModelling is a free and open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by Université Gustave Eiffel and CNRS
 * <http://noise-planet.org/noisemodelling.html>
 * as part of:
 * the Eval-PDU project (ANR-08-VILL-0005) 2008-2011, funded by the Agence Nationale de la Recherche (French)
 * the CENSE project (ANR-16-CE22-0012) 2017-2021, funded by the Agence Nationale de la Recherche (French)
 * the Nature4cities (N4C) project, funded by European Union’s Horizon 2020 research and innovation programme under grant agreement No 730468
 *
 * Noisemap is distributed under GPL 3 license.
 *
 * Contact: contact@noise-planet.org
 *
 * Copyright (C) 2011-2012 IRSTV (FR CNRS 2488) and Ifsttar
 * Copyright (C) 2013-2019 Ifsttar and CNRS
 * Copyright (C) 2020 Université Gustave Eiffel and CNRS
 */

package org.noise_planet.noisemodelling.wps

import org.h2gis.functions.io.shp.SHPRead
import org.noise_planet.noisemodelling.wps.Database_Manager.Add_Primary_Key
import org.noise_planet.noisemodelling.wps.Database_Manager.Clean_Database
import org.noise_planet.noisemodelling.wps.Database_Manager.Display_Database
import org.noise_planet.noisemodelling.wps.Database_Manager.Drop_a_Table
import org.noise_planet.noisemodelling.wps.Database_Manager.Table_Visualization_Data
import org.noise_planet.noisemodelling.wps.Database_Manager.Table_Visualization_Map
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Statement

/**
 * Test parsing of zip file using H2GIS database
 */
class TestDatabaseManager extends JdbcTestCase {
    Logger LOGGER = LoggerFactory.getLogger(TestDatabaseManager.class)


    void testAddPrimaryKey1() {
        SHPRead.readShape(connection, TestDatabaseManager.getResource("receivers.shp").getPath())
        Statement stmt = connection.createStatement()
        stmt.execute("ALTER TABLE receivers DROP PRIMARY KEY;")

        String res = new Add_Primary_Key().exec(connection,
                ["pkName": "ID",
                 "tableName" : "receivers"])

        assertEquals("RECEIVERS has a new primary key column which is called ID.", res)
    }

    void testAddPrimaryKey2() {
        SHPRead.readShape(connection, TestDatabaseManager.getResource("receivers.shp").getPath())
        Statement stmt = connection.createStatement()
        stmt.execute("ALTER TABLE receivers DROP PRIMARY KEY;")

        String res = new Add_Primary_Key().exec(connection,
                ["pkName": "PK",
                 "tableName" : "receivers"])

        assertEquals("RECEIVERS has a new primary key constraint on PK.", res)
    }

    void testAddPrimaryKey3() {
        SHPRead.readShape(connection, TestDatabaseManager.getResource("receivers.shp").getPath())

        String res = new Add_Primary_Key().exec(connection,
                ["pkName": "PK",
                 "tableName" : "receivers"])

        assertEquals("Warning : Source table RECEIVERS did already contain a primary key. The constraint has been removed. </br>RECEIVERS has a new primary key constraint on PK.", res)
    }

    void testCleanDatabase() {
        SHPRead.readShape(connection, TestDatabaseManager.getResource("receivers.shp").getPath())

        String res = new Clean_Database().exec(connection,
                ["areYouSure": true ])

        assertEquals("The table(s) RECEIVERS was/were dropped.", res)
    }

    void testDropTable() {
        SHPRead.readShape(connection, TestDatabaseManager.getResource("receivers.shp").getPath())

        String res = new Drop_a_Table().exec(connection,
                ["tableToDrop": "receivers" ])

        assertEquals("The table RECEIVERS was dropped !", res)
    }


    void testDisplayTables1() {
        SHPRead.readShape(connection, TestDatabaseManager.getResource("buildings.shp").getPath())
        String res = new Display_Database().exec(connection, [])
        assertEquals("BUILDINGS</br></br>", res)
    }

    void testTableVisualizationMap() {
        SHPRead.readShape(connection, TestDatabaseManager.getResource("receivers.shp").getPath())
        String res = new Table_Visualization_Map().exec(connection,
                ["tableName": "receivers" ])
        assertEquals("MULTIPOINT ((-3.365136315933348 47.74196183479599), (-3.3644713054746362 47.7419980699039), (-3.3638062941111277 47.74203430123337), (-3.3631412818429163 47.74207052878434), (-3.362476268670094 47.742106752556886), (-3.3618112545927565 47.74214297255093), (-3.3611462396109917 47.74217918876648), (-3.360481223724894 47.742215401203524), (-3.359816206934557 47.7422516098621), (-3.3591511892400723 47.74228781474215), (-3.35848617064153 47.74232401584368), (-3.3578211511390252 47.74236021316668), (-3.357156130732651 47.74239640671113), (-3.356491109422499 47.74243259647708), (-3.3558260872086616 47.74246878246446), (-3.355161064091231 47.74250496467329), (-3.3544960400703 47.74254114310354), (-3.3538310151459614 47.74257731775524), (-3.353165989318307 47.74261348862837), (-3.35250096258743 47.74264965572289), (-3.3518359349534226 47.74268581903885), (-3.351170906416377 47.74272197857617), (-3.3505058769763867 47.7427581343349), (-3.3498408466335423 47.742794286315046), (-3.3491758153879374 47.74283043451652), (-3.348510783239665 47.742866578939385), (-3.3478457501888186 47.742902719583604), (-3.347180716235488 47.7429388564492), (-3.346515681379767 47.742974989536144), (-3.345850645621748 47.74301111884441), (-3.3451856089615255 47.74304724437402), (-3.344520571399189 47.74308336612493), (-3.3438555329348314 47.743119484097186), (-3.365190039017174 47.74241039737458), (-3.3645250229943398 47.742446632778716), (-3.3638600060666874 47.74248286440436), (-3.36319498823431 47.7425190922515), (-3.3625299694973 47.74255531632014), (-3.361864949855747 47.742591536610284), (-3.3611999293097474 47.7426277531219), (-3.3605349078593942 47.74266396585502), (-3.359869885504774 47.74270017480956), (-3.3592048622459845 47.74273637998559), (-3.3585398380831175 47.74277258138306), (-3.3578748130162643 47.74280877900195), (-3.3572097870455178 47.74284497284232), (-3.356544760170972 47.74288116290409), (-3.355879732392715 47.74291734918729), (-3.3552147037108453 47.7429535316919), (-3.3545496741254506 47.74298971041795), (-3.3538846436366256 47.74302588536537), (-3.353219612244463 47.74306205653416), (-3.352554579949055 47.74309822392435), (-3.3518895467504928 47.74313438753594), (-3.3512245126488702 47.74317054736888), (-3.35055947764428 47.74320670342319), (-3.349894441736813 47.74324285569885), (-3.349229404926564 47.74327900419586), (-3.3485643672136236 47.74331514891421), (-3.3478993285980865 47.7433512898539), (-3.347234289080042 47.74338742701487), (-3.346569248659585 47.743423560397204), (-3.345904207336807 47.743459690000826), (-3.345239165111802 47.743495815825774), (-3.3445741219846594 47.74353193787199), (-3.3439090779554763 47.74356805613951), (-3.3652437630059113 47.742858959817646), (-3.3645787414188644 47.74289519551804), (-3.3639137189269745 47.74293142743988), (-3.361918646023188 47.74304010053417), (-3.359258536155977 47.74318494509355), (-3.3585935064286914 47.743221146786944), (-3.3572634442621854 47.743293538838024), (-3.3565984118231498 47.74332972919566), (-3.3559333784803855 47.743365915774675), (-3.355268344233982 47.74340209857508), (-3.3546033090840317 47.743438277596866), (-3.353938273030628 47.743474452840026), (-3.3532732360738655 47.743510624304534), (-3.3526081982138334 47.74354679199039), (-3.3519431594506246 47.743582955897615), (-3.3512781197843315 47.74361911602617), (-3.3506130792150493 47.743655272376046), (-3.3499480377428674 47.743691424947244), (-3.349282995367881 47.74372757373976), (-3.3486179520901795 47.743763718753605), (-3.3479529079098587 47.74379985998872), (-3.3472878628270077 47.74383599744516), (-3.3466228168417227 47.74387213112287), (-3.345957769954093 47.74390826102188), (-3.345292722164213 47.74394438714213), (-3.3446276734721754 47.743980509483656), (-3.343962623878071 47.74401662804643), (-3.3652974878995807 47.74330752212526), (-3.364632460748228 47.743343758121846), (-3.3639674326920095 47.74337999033988), (-3.3626373738653523 47.743452443440226), (-3.361972343095099 47.74348866432256), (-3.359977245357744 47.74359730429805), (-3.3593122109700704 47.74363351006603), (-3.3586471756782728 47.74366971205539), (-3.357982139482444 47.74370591026612), (-3.3573171023826758 47.743742104698256), (-3.356652064379062 47.74377829535174), (-3.3559870254716944 47.74381448222659), (-3.3553219856606655 47.743850665322796), (-3.354656944946068 47.74388684464034), (-3.353991903327992 47.74392302017922), (-3.351996773053838 47.74403152412383), (-3.350001634651727 47.74413999406021), (-3.3493365867119103 47.74417614314822), (-3.3486715378693575 47.744212288457554), (-3.3480064881241587 47.744248429988126), (-3.3473414374764103 47.744284567739975), (-3.3466763859262034 47.7443207017131), (-3.3460113334736303 47.74435683190747), (-3.344681225861755 47.74442908095989), (-3.34401617070264 47.74446519981792), (-3.3646861809824538 47.74379232059008), (-3.3626910774062466 47.74390100679701), (-3.3620260410715037 47.743937227975415), (-3.361361003832244 47.743973445375254), (-3.360030926640542 47.744045868839066), (-3.359365886688287 47.74408207490299), (-3.3580358040714304 47.74415447569498), (-3.357370761407013 47.744190670422974), (-3.3560406733666643 47.74426304854297), (-3.355375627990916 47.74429923193499), (-3.3547105817115783 47.744335411548306), (-3.35404553452874 47.744371587382915), (-3.353380486442497 47.74440775943883), (-3.352715437452938 47.74444392771604), (-3.352050387560159 47.74448009221452), (-3.3507202850653046 47.74455240987535), (-3.350055232463416 47.744588563037674), (-3.3493901789586764 47.74462471242121), (-3.3487251245511778 47.74466085802601), (-3.3480600692410114 47.74469699985207), (-3.3473950130282715 47.74473313789935), (-3.346729955913051 47.74476927216786), (-3.3460648978954404 47.74480540265758), (-3.3453998389755353 47.74484152936851), (-3.3447347791534248 47.74487765230065), (-3.3440697184292048 47.744913771453966), (-3.365404940401812 47.744204646333735), (-3.3634098228464455 47.74431334476502), (-3.3627447818517515 47.7443495700182), (-3.362079739952425 47.74438579149273), (-3.36141469714856 47.74442200918861), (-3.36008460882758 47.74449443324449), (-3.359419563310651 47.74453063960441), (-3.3587545168895527 47.74456684218566), (-3.358089469564378 47.74460304098823), (-3.3574244213352187 47.74463923601215), (-3.356759372202168 47.74467542725734), (-3.355429271224759 47.74474779841164), (-3.3547642193805878 47.74478397832072), (-3.354099166632895 47.74482015445108), (-3.3534341129817715 47.744856326802704), (-3.352769058427312 47.7448924953756), (-3.3521040029696096 47.74492866016975), (-3.3514389466087535 47.744964821185114), (-3.3507738893448376 47.74500097842175), (-3.3501088311779568 47.74503713187961), (-3.349443772108201 47.745073281558696), (-3.3487787121356636 47.74510942745897), (-3.3481136512604377 47.745145569580494), (-3.3474485894826134 47.74518170792319), (-3.346783526802287 47.7452178424871), (-3.346118463219548 47.74525397327219), (-3.345453398734489 47.74529010027847), (-3.3447883333472053 47.7453262235059), (-3.3441232670577867 47.745362342954515), (-3.3641285794158327 47.74472567822651), (-3.3634635337612333 47.74476190755446), (-3.362798487201887 47.74479813310379), (-3.362133439737886 47.74483435487445), (-3.360803342096289 47.74490678707969), (-3.3601382919188785 47.744942997514315), (-3.3594732408371852 47.744979204170235), (-3.358808188851297 47.745015407047454), (-3.3581431359613103 47.74505160614597), (-3.357478082167316 47.74508780146576), (-3.3568130274694084 47.745123993006814), (-3.3554829153622174 47.74519636475274), (-3.35481785795312 47.745232544957595), (-3.354152799640478 47.74526872138369), (-3.353487740424385 47.74530489403101), (-3.3528226803049304 47.74534106289957), (-3.3521576192822096 47.74537722798938), (-3.351492557356315 47.74541338930039), (-3.350827494527338 47.74544954683259), (-3.3501624307953715 47.74548570058599), (-3.349497366160507 47.74552185056063), (-3.348832300622839 47.74555799675642), (-3.3481672341824593 47.745594139173406), (-3.347502166839461 47.74563027781153), (-3.346837098593935 47.74566641267083), (-3.346172029445974 47.74570254375128), (-3.3455069593956708 47.74573867105289), (-3.34484188844312 47.74577479457565), (-3.344176816588412 47.745810914319556), (-3.365512396524051 47.74510176999984), (-3.3615220864945545 47.74531913640857), (-3.3608570316567086 47.74535535091794), (-3.360191975914463 47.74539156164855), (-3.3595269192679096 47.74542776860047), (-3.356866683640471 47.745572558620694), (-3.356201622473767 47.74560874667884), (-3.355536560403311 47.745644930958264), (-3.354871497429196 47.74568111145888), (-3.3542064335515134 47.745717288180714), (-3.353541368770356 47.74575346112375), (-3.3528763030858166 47.745789630288), (-3.3522112364979852 47.74582579567345), (-3.3515461690069586 47.74586195728007), (-3.350881100612827 47.745898115107885), (-3.3502160313156835 47.74593426915684), (-3.3495509611156193 47.745970419426996), (-3.3488858900127276 47.7460065659183), (-3.348220818007102 47.74604270863072), (-3.3475557450988345 47.746078847564284), (-3.3468906712880164 47.746114982719014), (-3.346225596574741 47.74615111409484), (-3.345560520959103 47.74618724169179), (-3.344895444441191 47.74622336550986), (-3.3442303670211007 47.74625948554905), (-3.3655661259427316 47.74555033162941), (-3.3649010709684597 47.74558656910712), (-3.3642360150892086 47.745622802806174), (-3.362905900616145 47.745695258868075), (-3.362240842022517 47.74573148123097), (-3.36157578252428 47.74576769981512), (-3.3609107221215293 47.74580391462051), (-3.3602456608143534 47.74584012564715), (-3.3582504714672194 47.74594873605454), (-3.356920340715378 47.74602112409893), (-3.3562552739836105 47.74605731245295), (-3.3555902063480665 47.746093497028156), (-3.3549251378088396 47.74612967782454), (-3.3542600683660244 47.74616585484214), (-3.3535949980197093 47.74620202808089), (-3.3529299267699915 47.746238197540805), (-3.3522648546169593 47.74627436322192), (-3.351599781560707 47.74631052512418), (-3.3509347076013283 47.74634668324756), (-3.3502696327389136 47.74638283759208), (-3.349604556973557 47.74641898815774), (-3.3489394803053503 47.74645513494453), (-3.348274402734386 47.746491277952465), (-3.3476093242607576 47.74652741718146), (-3.3469442448845554 47.746563552631585), (-3.3462791646058756 47.746599684302794), (-3.3456140834248056 47.74663581219513), (-3.344949001341443 47.7466719363085), (-3.344283918355876 47.746708056642944), (-3.3656198562664814 47.7459988931233), (-3.364954795727348 47.746035130897255), (-3.364289734283213 47.74607136489249), (-3.363624671934172 47.74610759510902), (-3.362959608680314 47.74614382154674), (-3.362294544521732 47.74618004420573), (-3.3616294794585206 47.74621626308595), (-3.359634278842028 47.746324897053974), (-3.3589692101612183 47.74636110081907), (-3.357639070087189 47.74643349701286), (-3.356308926397227 47.746505878091355), (-3.355643853196503 47.746542062962384), (-3.354978779092074 47.74657824405456), (-3.354313704084032 47.7466144213679), (-3.3536486281724693 47.74665059490236), (-3.352983551357479 47.74668676465797), (-3.3523184736391527 47.746722930634746), (-3.3516533950175837 47.74675909283262), (-3.3509883154928644 47.74679525125161), (-3.350323235065088 47.7468314058917), (-3.349658153734346 47.7468675567529), (-3.3489930715007303 47.7469037038352), (-3.3483279883643355 47.74693984713856), (-3.3476629043252526 47.746975986663024), (-3.3469978193835748 47.74701212240858), (-3.345667646792804 47.74708438256282), (-3.3450025591438948 47.74712050697153), (-3.344337470592763 47.74715662760126), (-3.365673587495325 47.74644745448145), (-3.3650085213912377 47.74648369255168), (-3.3643434543821265 47.746519926843106), (-3.3636783864680853 47.74655615735578), (-3.3630133176492056 47.74659238408968), (-3.3623482479255786 47.7466286070448), (-3.359687959985466 47.7467734610772), (-3.359022885739497 47.746809665138265), (-3.358357810589338 47.74684586542048), (-3.357692734535082 47.74688206192389), (-3.3563625797146424 47.74695444359412), (-3.3556975009486467 47.74699062876097), (-3.354367340705559 47.747062987758014), (-3.353702259228657 47.747099161588196), (-3.3530371768483 47.74713533163952), (-3.3523720935645884 47.74717149791191), (-3.3517070093776113 47.747207660405415), (-3.3510419242874585 47.747243819119994), (-3.3503768382942267 47.747279974055644), (-3.349711751398006 47.747316125212386), (-3.3490466635988905 47.7473522725902), (-3.3483815748969725 47.74738841618905), (-3.347716485292344 47.74742455600897), (-3.347051394785096 47.74746069204991), (-3.345721211063119 47.74753295279489), (-3.3450561178485723 47.74756907749891), (-3.344391023731781 47.74760519842394), (-3.3657273196292827 47.74689601570387), (-3.3650622479601497 47.74693225407033), (-3.364397175385969 47.74696848865798), (-3.363732101906837 47.74700471946685), (-3.3630670275228414 47.747040946496895), (-3.3617368760406388 47.74711338922052), (-3.3610717989426147 47.7471496049141), (-3.360406720940101 47.747185816828825), (-3.3584114815065345 47.747294429899924), (-3.357081317363398 47.74736681971965), (-3.3564162339358785 47.747403008961165), (-3.355751149604517 47.747439194423805), (-3.355086064369403 47.74747537610756), (-3.354420978230631 47.747511554012384), (-3.3537558911882934 47.7475477281383), (-3.353090803242482 47.74758389848534), (-3.35242571439329 47.74762006505337), (-3.351760624640809 47.7476562278425), (-3.351095533985132 47.74769238685271), (-3.3504304424263527 47.74772854208392), (-3.3497653499645623 47.747764693536226), (-3.3491002565998533 47.747800841209525), (-3.348435162332319 47.74783698510384), (-3.3477700671620516 47.74787312521919), (-3.3471049710891436 47.74790926155557), (-3.3451096774554987 47.74801764789064), (-3.3444445777729537 47.74805376911096), (-3.3657810526683813 47.7473445767905), (-3.3651159754341085 47.747380815453205), (-3.3644508972947675 47.747417050337084), (-3.3631207383012485 47.74748950876832), (-3.362455657447254 47.74752573231569), (-3.36046040945745 47.74763438028459), (-3.3597953249852157 47.747670588716474), (-3.3591302396086515 47.74770679336949), (-3.3584651533278533 47.74774299424359), (-3.355804799164139 47.74788775995094), (-3.3551397083635432 47.747923941930466), (-3.3544746166592696 47.74796012013108), (-3.353809524051406 47.74799629455271), (-3.353144430540045 47.74803246519542), (-3.3524793361252807 47.74806863205914), (-3.351814240807205 47.7481047951439), (-3.3511491445859116 47.74814095444968), (-3.3504840474614914 47.7481771099765), (-3.349818949434037 47.748213261724324), (-3.3491538505036433 47.74824940969313), (-3.3484887506703997 47.74828555388296), (-3.3478236499344014 47.74832169429375), (-3.3471585482957384 47.748357830925535), (-3.345828342310793 47.748430092851976), (-3.345163237964696 47.74846621814668), (-3.3444981327163044 47.74850233966231), (-3.3658347866126395 47.747793137741354), (-3.365169703813137 47.74782937670029), (-3.3631744499844456 47.74793807090397), (-3.36250936356513 47.747974294747486), (-3.361844276241092 47.74801051481206), (-3.3578537333028864 47.748227755842606), (-3.357188639648387 47.74826394945479), (-3.356523545089906 47.74830013928802), (-3.3558584496275348 47.748336325342315), (-3.3551933532613667 47.748372507617624), (-3.3545282559914944 47.74840868611398), (-3.353863157818012 47.748444860831356), (-3.3531980587410097 47.74848103176973), (-3.352532958760581 47.748517198929136), (-3.3518678578768193 47.748553362309536), (-3.351202756089816 47.74858952191096), (-3.350537653399662 47.748625677733344), (-3.349872549806454 47.748661829776694), (-3.349207445310281 47.748697978041015), (-3.348542339911236 47.7487341225263), (-3.3478772336094136 47.74877026323256), (-3.3472121264049046 47.74880640015977), (-3.3452167993761863 47.74891478826701), (-3.344551688561857 47.748950910077916), (-3.3658885214620833 47.74824169855634), (-3.3652234330972557 47.748277937811544), (-3.363893253652351 47.74835040498529), (-3.3625630705877265 47.74842285704344), (-3.3618979776982507 47.748459077404135), (-3.3612328839041234 47.748495293985904), (-3.359902693602281 47.74856771581265), (-3.3592375970947517 47.748603921057594), (-3.3585724996829405 47.74864012252359), (-3.3579074013669397 47.74867632021063), (-3.357242302146843 47.74871251411868), (-3.3559121009947264 47.74878489059789), (-3.355246999062893 47.74882107316899), (-3.3545818962273337 47.74885725196111), (-3.3539167924881386 47.748893426974206), (-3.353251687845402 47.74892959820831), (-3.352586582299217 47.748965765663385), (-3.3519214758496747 47.74900192933943), (-3.351256368496868 47.749038089236436), (-3.35059126024089 47.749074245354386), (-3.3499261510818323 47.74911039769327), (-3.3492610410197896 47.74914654625316), (-3.348595930054852 47.74918269103393), (-3.347930818187112 47.749218832035645), (-3.347265705416664 47.749254969258246), (-3.3466005917436 47.74929110270179), (-3.3459354771680108 47.74932723236623), (-3.3452703616899915 47.74936335825158), (-3.344605245309633 47.749399480357795), (-3.365942257216735 47.7486902592355), (-3.3612865807003804 47.74894385673818), (-3.3599563792673637 47.74901627915699), (-3.359291277194213 47.749052484697906), (-3.358626174216759 47.74908868645987), (-3.3579610703350897 47.74912488444282), (-3.3572959655493024 47.749161078646765), (-3.3559657532657376 47.749233455717636), (-3.3553006457681462 47.749269638584536), (-3.3546355373668053 47.74930581767242), (-3.3539704280618072 47.74934199298125), (-3.3533053178532435 47.74937816451105), (-3.352640206741209 47.749414332261786), (-3.3519750947257947 47.74945049623349), (-3.351309981807093 47.749486656426114), (-3.3506448679851966 47.74952281283965), (-3.349979753260199 47.7495589654741), (-3.349314637632192 47.74959511432947), (-3.348649521101268 47.74963125940573), (-3.3479844036675197 47.749667400702904), (-3.34731928533104 47.74970353822097), (-3.346654166091921 47.7497396719599), (-3.345989045950256 47.7497758019197), (-3.3453239249061357 47.74981192810038), (-3.3446588029596556 47.7498480505019), (-3.365995993876616 47.749138819778764), (-3.365330894380864 47.749175059626495), (-3.364665793979951 47.74921129569525), (-3.3640006926739714 47.749247527985084), (-3.363335590463015 47.74928375649592), (-3.362670487347177 47.74931998122779), (-3.3620053833265477 47.74935620218069), (-3.361340278401222 47.7493924193546), (-3.360010065836844 47.74946484236543), (-3.357349629855789 47.74960964303899), (-3.356684518600169 47.74964583375979), (-3.356019406440592 47.74968202070154), (-3.355354293377149 47.74971820386423), (-3.3546891794099345 47.74975438324788), (-3.35402406453904 47.74979055885248), (-3.3533589487645576 47.749826730677995), (-3.3526938320865804 47.749862898724395), (-3.352028714505201 47.749899062991716), (-3.3513635960205117 47.749935223479945), (-3.3506984766326067 47.74997138018907), (-3.3500333563415747 47.75000753311909), (-3.349368235147512 47.75004368226997), (-3.348703113050509 47.75007982764174), (-3.348037990050659 47.75011596923435), (-3.347372866148056 47.750152107047846), (-3.346707741342789 47.75018824108219), (-3.346042615634954 47.750224371337346), (-3.3453774890246417 47.750260497813386), (-3.344712361511945 47.75029662051019), (-3.366049731441751 47.74958738018611), (-3.3647195204138605 47.74965985669508), (-3.363389305765607 47.7497323180881), (-3.3627241970840767 47.749768543116126), (-3.362059087497732 47.749804764365145), (-3.3613939770066668 47.74984098183513), (-3.360063753310746 47.74991340543804), (-3.358068410983772 47.7500220124996), (-3.3574032950663266 47.75005820729533), (-3.356738178244809 47.75009439831198), (-3.3560730605193116 47.75013058554957), (-3.3554079418899248 47.750166769008075), (-3.354742822356744 47.750202948687495), (-3.3540777019198598 47.75023912458781), (-3.353412580579365 47.750275296709006), (-3.3527474583353536 47.750311465051126), (-3.3520823351879163 47.75034762961408), (-3.351417211137148 47.75038379039793), (-3.3507520861831384 47.75041994740265), (-3.3500869603259833 47.75045610062822), (-3.3494218335657706 47.75049225007464), (-3.348756705902597 47.750528395741895), (-3.3467613174962265 47.75063681006863), (-3.346096186222129 47.75067294061917), (-3.345431054045532 47.75070906739052), (-3.3447659209665277 47.75074519038269), (-3.366103469912161 47.750035940457494), (-3.3647732477528596 47.75010841755897), (-3.3641081353154934 47.750144650441186), (-3.363443021973105 47.75018087954439), (-3.3621127925736363 47.75025332641365), (-3.361447676516741 47.75028954417973), (-3.3601174416890887 47.75036196837469), (-3.359452322918518 47.75039817480359), (-3.35812208266435 47.7504705763241), (-3.3574569611809375 47.75050677141575), (-3.3567918387934284 47.75054296272829), (-3.3561267155019174 47.7505791502617), (-3.355461591306496 47.75061533401602), (-3.354796466207256 47.75065151399121), (-3.354131340204289 47.75068769018726), (-3.3534662132976907 47.750723862604175), (-3.3528010854875525 47.75076003124194), (-3.352135956773967 47.75079619610056), (-3.3514708271570246 47.750832357180045), (-3.3508056966368196 47.750868514480345), (-3.3501405652134446 47.75090466800149), (-3.349475432886993 47.75094081774341), (-3.3488102996575555 47.75097696370616), (-3.3481451655252243 47.75101310588971), (-3.346149757711806 47.75112150976512), (-3.3454846199688295 47.75115763683183), (-3.36615720928787 47.75048450059293), (-3.36349673908553 47.75062944086467), (-3.362831619272337 47.75066566648499), (-3.361501376931465 47.75073810638838), (-3.358840881394378 47.75088294084605), (-3.3581757552491163 47.750919140012726), (-3.357510628199643 47.75095533540022), (-3.3568455002460516 47.75099152700863), (-3.356180371388435 47.7510277148379), (-3.3555152416268843 47.75106389888802), (-3.3548501109614928 47.751100079158974), (-3.3541849793923526 47.75113625565079), (-3.353519846919558 47.751172428363425), (-3.3521895792633702 47.75124476245116), (-3.3515244440801633 47.75128092382623), (-3.350859307993669 47.75131708142212), (-3.3501941710039844 47.751353235238824), (-3.349529033111199 47.75138938527626), (-3.3488638943154045 47.751425531534544), (-3.348198754616696 47.75146167401356), (-3.3475336140151635 47.751497812713346), (-3.346868472510902 47.75153394763392), (-3.346203330104003 47.751570078775195), (-3.3455381867945566 47.75160620613721), (-3.34487304258266 47.75164232972002), (-3.3662109495688988 47.750933060592345), (-3.3655458278101866 47.750969301625126), (-3.364880705146221 47.75100553887882), (-3.364215581577098 47.75104177235346), (-3.362220205439698 47.751150450102784), (-3.3602248211591967 47.751259093840154), (-3.3595596912565497 47.75129530086102), (-3.3588945604494844 47.75133150410273), (-3.3582294287380914 47.75136770356533), (-3.357564296122467 47.75140389924874), (-3.356899162602701 47.751440091153015), (-3.3562340281788843 47.75147627927811), (-3.355568892851114 47.75151246362404), (-3.354903756619477 47.751548644190784), (-3.3542386194840708 47.751584820978344), (-3.3535734814449873 47.75162099398669), (-3.352243202656153 47.75169332866578), (-3.3515780619065882 47.75172949033647), (-3.350912920253714 47.751765648227945), (-3.350247777697626 47.75180180234021), (-3.3495826342384136 47.75183795267321), (-3.348252344610991 47.751910242001486), (-3.347587198442963 47.7519463809967), (-3.346922051372185 47.75198251621266), (-3.3462569033987437 47.75201864764932), (-3.345591754522737 47.752054775306725), (-3.344926604744254 47.75209089918479), (-3.3662646907552727 47.75138162045573), (-3.3655995634305884 47.75141786178479), (-3.36426930606549 47.751490333105565), (-3.3636041760252597 47.75152656309729), (-3.3629390450800316 47.75156278930989), (-3.361608780474958 47.75163523039766), (-3.3609436468152953 47.75167144527284), (-3.359613376782182 47.75174386368573), (-3.3589482404089175 47.751780067223436), (-3.358283103131303 47.751816266981926), (-3.3576179649494318 47.75185246296129), (-3.3569528258633974 47.75188865516143), (-3.3562876858732915 47.751924843582366), (-3.3556225449792056 47.75196102822409), (-3.3549574031812353 47.751997209086625), (-3.3542922604794696 47.75203338616993), (-3.352296826952336 47.752141894744405), (-3.35163168063632 47.75217805671076), (-3.3509665334169743 47.75221421489785), (-3.350301385294389 47.75225036930565), (-3.3496362362686583 47.75228651993419), (-3.3476407837735156 47.75239494914408), (-3.346975631136126 47.75243108465548), (-3.346310477596054 47.75246721638753), (-3.3456453231533922 47.75250334434026), (-3.344980167808231 47.75253946851366), (-3.366318432847015 47.75183018018307), (-3.3656532999562665 47.7518664218084), (-3.36498816616022 47.75190265965457), (-3.36432303145897 47.75193889372163), (-3.3636578958526067 47.751975124009526), (-3.3629927593412248 47.752011350518266), (-3.3623276219249156 47.752047573247864), (-3.3616624836037716 47.752083792198285), (-3.36033220424735 47.75215621876156), (-3.359667063212257 47.75219242637442), (-3.3583367784287703 47.752264830262554), (-3.3576716346805617 47.75230102653778), (-3.357006490028167 47.75233721903379), (-3.356341344471677 47.752373407750596), (-3.3556761980111856 47.75240959268814), (-3.3550110506467847 47.75244577384644), (-3.3543459023785673 47.75248195122548), (-3.3523504521519416 47.75259046068708), (-3.3516853002693843 47.75262662294902), (-3.3510201474834718 47.752662781431724), (-3.3503549937942982 47.75269893613509), (-3.3496898392019565 47.75273508705919), (-3.3476943700068436 47.75284351715552), (-3.3456988926865443 47.75295191323783), (-3.3450337317746133 47.75298803770656), (-3.366372175844145 47.752278739774304), (-3.36570703738724 47.75231498169591), (-3.3630464745073456 47.75245991159059), (-3.362381331524764 47.752496134616294), (-3.361716187637327 47.75253235386283), (-3.361051042845125 47.75256856933011), (-3.3597207505467956 47.752640988927055), (-3.3583904546305163 47.75271339340708), (-3.357725305315878 47.75274958997822), (-3.35706015509703 47.75278578277013), (-3.3563950039740638 47.75282197178275), (-3.355729851947074 47.752858157016135), (-3.3550646990161517 47.75289433847023), (-3.3543995451813897 47.752930516145035), (-3.353734390442882 47.75296669004055), (-3.3530692348007194 47.75300286015678), (-3.352404078254995 47.753039026493674), (-3.351738920805802 47.75307518905128), (-3.3510737624532325 47.753111347829574), (-3.3504086031973763 47.753147502828526), (-3.349743443038331 47.753183654048144), (-3.347747957142971 47.7532920850309), (-3.3457524631222175 47.753400481999385), (-3.3450872966434244 47.75343660676345), (-3.366425919746689 47.752727299229434), (-3.365760775723535 47.75276354144729), (-3.3650956307950386 47.75279977988598), (-3.3617698925756483 47.752980915391255), (-3.361104742217037 47.7530171311546), (-3.3604395909537312 47.75305334313871), (-3.359774438785821 47.75308955134361), (-3.359109285713403 47.753125755769204), (-3.358444131736565 47.753161956415525), (-3.357778976855403 47.75319815328259), (-3.35711382107001 47.75323434637036), (-3.3564486643804754 47.75327053567885), (-3.355783506786895 47.75330672120805), (-3.3551183482893583 47.75334290295791), (-3.3544531888879603 47.75337908092848), (-3.3537880285827923 47.75341525511974), (-3.353122867373947 47.753451425531665), (-3.3524577052615174 47.75348759216422), (-3.351792542245596 47.75352375501747), (-3.351127378326275 47.75355991409135), (-3.3504622135036466 47.753596069385864), (-3.3497970477778054 47.75363222090104), (-3.349131881148841 47.75366836863683), (-3.3478015451819183 47.75374065277026), (-3.34647120560362 47.75381292178611), (-3.3458060344604363 47.75384905062492), (-3.3451408624146857 47.753885175684296), (-3.366479664554668 47.75317585854839), (-3.365814514965173 47.753212101062566), (-3.364484213070177 47.75328457475319), (-3.363153907554458 47.75335703332689), (-3.362488753439059 47.75339325694486), (-3.361158442493644 47.753465692842994), (-3.360493285663813 47.75350190512315), (-3.359828127929357 47.75353811362402), (-3.359162969290368 47.7535743183456), (-3.35849780974694 47.75361051928787), (-3.357832649299163 47.75364671645085), (-3.35716748794713 47.753682909834495), (-3.356502325690934 47.753719099438854), (-3.3558371625306704 47.75375528526384), (-3.355171998466427 47.75379146730952), (-3.3545068334982986 47.75382764557586), (-3.3538416676263783 47.75386382006283), (-3.3531765008507586 47.75389999077044), (-3.3525113331715324 47.75393615769869), (-3.35184616458879 47.75397232084757), (-3.3511809951026263 47.75400848021707), (-3.3505158247131313 47.75404463580717), (-3.3498506534204 47.75408078761789), (-3.3491854812245263 47.754116935649186), (-3.3485203081255976 47.75415307990107), (-3.3478551341237113 47.75418922037354), (-3.3458596067012203 47.75429761911438), (-3.3451944290884215 47.75433374446909), (-3.3665334102681066 47.75362441773121), (-3.3658682551121766 47.75366066054167), (-3.3652030990508583 47.75369689957284), (-3.3645379420842434 47.75373313482479), (-3.3638727842124263 47.753769366297405), (-3.3632076254354972 47.75380559399081), (-3.3625424657535494 47.753841817904906), (-3.3618773051666757 47.75387803803971), (-3.3612121436749702 47.753914254395205), (-3.3592166537717754 47.754022880785826), (-3.35855148866166 47.754059082024064), (-3.356555987905464 47.754167663062695), (-3.3558908191784225 47.75420384918353), (-3.354560479012431 47.754276210087106), (-3.353895307573666 47.7543123848698), (-3.353230135231178 47.75434855587315), (-3.35256496198506 47.75438472309703), (-3.351899787835406 47.75442088654155), (-3.351234612782305 47.75445704620666), (-3.3505694368258525 47.754493202092355), (-3.3499042599661397 47.7545293541986), (-3.349239082203261 47.754565502525416), (-3.348573903537306 47.75460164707281), (-3.34657836212192 47.754710058038235), (-3.3459131798445934 47.75474618746777), (-3.3452479966646536 47.75478231311781), (-3.3665871568870256 47.754072976777806), (-3.3659219961645688 47.75410921988454), (-3.364591672003514 47.75418169476016), (-3.3639265085651004 47.754217926529), (-3.3632613442215535 47.75425415451853), (-3.3625961789729657 47.75429037872877), (-3.3592703391576446 47.75447144308992), (-3.358605168480753 47.7545076446241), (-3.3579399968994674 47.75454384237892), (-3.3572748244138797 47.75458003635433), (-3.3566096510240855 47.75461622655042), (-3.355279301532242 47.75468859560433), (-3.3546141254303774 47.754724774462176), (-3.3539489484246747 47.75476094954063), (-3.3532837705152283 47.75479712083965), (-3.352618591702127 47.754833288359244), (-3.351953411985467 47.754869452099406), (-3.351288231365337 47.75490561206013), (-3.350623049841834 47.75494176824141), (-3.3492926840850705 47.75501406926554), (-3.3486274998519967 47.75505021410843), (-3.3466319417351165 47.7551586259601), (-3.345301565143405 47.755230881630396), (-3.3666409044114483 47.75452153568818), (-3.3646454028280104 47.7546302545593), (-3.3639802338229106 47.75466648662434), (-3.363315063912652 47.75470271491007), (-3.361319548751866 47.754811377091116), (-3.359324025448001 47.75492000525778), (-3.3579936720560584 47.75499240513867), (-3.3573284940035566 47.75502859941), (-3.356663315046824 47.7550647899019), (-3.3553329544210335 47.75513715954747), (-3.3546677727521623 47.755173338701084), (-3.35400259017943 47.75520951407526), (-3.3533374067029293 47.75524568566999), (-3.3526722223227536 47.75528185348528), (-3.3520070370389945 47.755318017521084), (-3.3513418508517456 47.75535417777741), (-3.3506766637610976 47.75539033425426), (-3.3466855222510388 47.755607193745846), (-3.346020328839197 47.75564332376612), (-3.345355134524698 47.75567945000685), (-3.366694652841397 47.75497009446226), (-3.3660294809856097 47.75500633816159), (-3.3653643082243647 47.755042578081586), (-3.3633687845088156 47.755151275165325), (-3.3620384308395255 47.755223720990955), (-3.361373252647481 47.75525993823469), (-3.359377712642865 47.755368567289445), (-3.3580473481169757 47.7554409677622), (-3.357382164497465 47.75547716232941), (-3.3560517945457726 47.75554954012552), (-3.355386608213779 47.75558572335437), (-3.3547214209778082 47.755621902803775), (-3.3540562328379533 47.75565807847371), (-3.3533910437943075 47.75569425036416), (-3.352725853846964 47.75573041847509), (-3.3520606629960135 47.75576658280655), (-3.3513954712415512 47.7558027433585), (-3.3507302785836672 47.75583890013093), (-3.3500650850224543 47.755875053123866), (-3.3493998905580074 47.75591120233724), (-3.3487346951904167 47.75594734777112), (-3.3467391036697105 47.75605576139541), (-3.346073904690474 47.756091891711066), (-3.345408704808557 47.75612801824713), (-3.3667484021768965 47.75541865310009), (-3.3660832247543047 47.755454897095724), (-3.365418046426234 47.755491137311964), (-3.364752867192774 47.75552737374881), (-3.3640876870540204 47.755563606406255), (-3.3634225060100644 47.75559983528433), (-3.3627573240609987 47.75563606038296), (-3.362092141206914 47.7556722817022), (-3.361426957447907 47.755708499242004), (-3.3607617727840666 47.75574471300239), (-3.3600965872154878 47.75578092298334), (-3.359431400742261 47.75581712918482), (-3.3587662133644804 47.755853331606865), (-3.358101025082238 47.75588953024947), (-3.3574358358956253 47.755925725112604), (-3.3561054548096645 47.755998103500374), (-3.3554402629105002 47.75603428702506), (-3.354775070107338 47.756070466770225), (-3.3541098764002673 47.75610664273591), (-3.353444681789385 47.75614281492207), (-3.3527794862747786 47.75617898332869), (-3.3521142898565457 47.75621514795579), (-3.3514490925347773 47.75625130880335), (-3.350783894309564 47.756287465871395), (-3.350118695181 47.75632361915987), (-3.349453495149179 47.75635976866881), (-3.34878829421419 47.756395914398176), (-3.348123092376128 47.75643205634795), (-3.347457889635086 47.756468194518156), (-3.3467926859911565 47.75650432890877), (-3.346127481444431 47.75654045951978), (-3.3454622759950023 47.756576586351215))", res)
    }

    void testTableVisualizationData() {
        SHPRead.readShape(connection, TestDatabaseManager.getResource("receivers.shp").getPath())
        String res = new Table_Visualization_Data().exec(connection,
                ["tableName": "receivers" ])
        assertTrue(res.contains("The total number of rows is 830"))
        assertTrue(res.contains("POINT (223495.9880411485 6757167.98900822 0)"))
    }
}
