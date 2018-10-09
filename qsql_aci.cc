

#include "qsql_aci.h"

#include <qcoreapplication.h>
#include <qvariant.h>
#include <qdatetime.h>
#include <qmetatype.h>
#include <qregexp.h>
#include <qshareddata.h>
#include <qsqlerror.h>
#include <qsqlfield.h>
#include <qsqlindex.h>
#include <qsqlquery.h>
#include <qstringlist.h>
#include <qvarlengtharray.h>
#include <qvector.h>
#include <qdebug.h>

#include "QSqlCachedResult.h"

#include <aci.h>
#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

#ifdef _DEBUG
#define QACI_DEBUG
#endif

#include <stdlib.h>

#define QACI_DYNAMIC_CHUNK_SIZE 65535
#define QACI_PREFETCH_MEM  10240

// setting this define will allow using a query from a different
// thread than its database connection.
// warning - this is not fully tested and can lead to race conditions
#define QACI_THREADED

#ifndef ACI_USE_UTF8
#define ACI_USE_UTF8 // this client driver user utf8
#endif

#ifndef ACE_UTF8ID
#define ACE_UTF8ID 871
#endif

Q_DECLARE_OPAQUE_POINTER(ACIEnv*);
Q_DECLARE_METATYPE(ACIEnv*)
Q_DECLARE_OPAQUE_POINTER(ACIStmt*);
Q_DECLARE_METATYPE(ACIStmt*)

QT_BEGIN_NAMESPACE

#if defined (ACI_USE_UTF8)
static const ub2 qOraCharset = ACE_UTF8ID;
#else
static const ub2 qOraCharset = ACI_UCS2ID;
#endif

typedef QVarLengthArray<sb2, 32> IndicatorArray;
typedef QVarLengthArray<ub2, 32> SizeArray;

static QByteArray qMakeOraDate(const QDateTime& dt);
static QDateTime qMakeDate(const char* oraDate);

static QByteArray qMakeACINumber(const qlonglong &ll, ACIError *err);
static QByteArray qMakeACINumber(const qulonglong& ull, ACIError* err);

static qlonglong qMakeLongLong(const char* ACINumber, ACIError* err);
static qulonglong qMakeULongLong(const char* ACINumber, ACIError* err);

static QString qOraWarn(ACIError *err, int *errorCode = 0);

#ifndef Q_CC_SUN
static // for some reason, Sun CC can't use qOraWarning when it's declared static
#endif
void qOraWarning(const char* msg, ACIError *err);
static QSqlError qMakeError(const QString& errString, QSqlError::ErrorType type, ACIError *err);



class QACIRowId : public QSharedData
{
public:
    QACIRowId(ACIEnv *env);
    ~QACIRowId();

    ACIRowid *id;

private:
    QACIRowId(const QACIRowId &other) : QSharedData(other) { Q_ASSERT(false); }
};

QACIRowId::QACIRowId(ACIEnv *env)
    : id(0)
{
    ACIDescriptorAlloc(env, reinterpret_cast<dvoid **>(&id),
        ACI_DTYPE_ROWID, 0, 0);
}

QACIRowId::~QACIRowId()
{
    if (id)
        ACIDescriptorFree(id, ACI_DTYPE_ROWID);
}

typedef QSharedDataPointer<QACIRowId> QACIRowIdPointer;
QT_BEGIN_INCLUDE_NAMESPACE
Q_DECLARE_METATYPE(QACIRowIdPointer)
QT_END_INCLUDE_NAMESPACE

class QACICols;
struct QACIResultPrivate;



class QACIResult : public QSqlCachedResult
{
    friend class QACIDriver;
    friend struct QACIResultPrivate;
    friend class QACICols;
public:
    QACIResult(const QACIDriver * db, const QACIDriverPrivate* p);
    ~QACIResult();
    bool prepare(const QString& query);
    bool exec();
    QVariant handle() const;

protected:
    bool gotoNext(ValueCache &values, int index);
    bool reset(const QString& query);
    int size();
    int numRowsAffected();
    QSqlRecord record() const;
    QVariant lastInsertId() const;
    bool execBatch(bool arrayBind = false);
    void virtual_hook(int id, void *data);

private:
    QACIResultPrivate * d;
};

struct QACIResultPrivate
{
    QACIResultPrivate(QACIResult *result, const QACIDriverPrivate *driver);
    ~QACIResultPrivate();

    QACICols *cols;
    QACIResult *q;
    ACIEnv *env;
    ACIError *err;
    ACISvcCtx *&svc;
    ACIStmt *sql;
    bool transaction;
    int serverVersion;
    int prefetchRows, prefetchMem;

    void setStatementAttributes();
    int bindValue(ACIStmt *sql, ACIBind **hbnd, ACIError *err, int pos,
        const QVariant &val, dvoid *indPtr, ub2 *tmpSize, QList<QByteArray> &tmpStorage);
    int bindValues(QVector<QVariant> &values, IndicatorArray &indicators, SizeArray &tmpSizes,
        QList<QByteArray> &tmpStorage);
    void outValues(QVector<QVariant> &values, IndicatorArray &indicators,
        QList<QByteArray> &tmpStorage);
    inline bool isOutValue(int i) const
    {
        return q->bindValueType(i) & QSql::Out;
    }
    inline bool isBinaryValue(int i) const
    {
        return q->bindValueType(i) & QSql::Binary;
    }

    void setCharset(dvoid* handle, ub4 type) const
    {
        int r = 0;
        Q_ASSERT(handle);

        r = ACIAttrSet(handle,
            type,
            // this const cast is safe since ACI doesn't touch
            // the charset.
            const_cast<void *>(static_cast<const void *>(&qOraCharset)),
            0,
            ACI_ATTR_CHARSET_ID,
            err);
        if (r != 0)
            qOraWarning("QACIResultPrivate::setCharsetI Couldn't set ACI_ATTR_CHARSET_ID: ", err);

    }
};

void QACIResultPrivate::setStatementAttributes()
{
    Q_ASSERT(sql);

    int r = 0;

    if (prefetchRows >= 0) {
        r = ACIAttrSet(sql,
            ACI_HTYPE_STMT,
            &prefetchRows,
            0,
            ACI_ATTR_PREFETCH_ROWS,
            err);
        if (r != 0)
            qOraWarning("QACIResultPrivate::setStatementAttributes:"
                " Couldn't set ACI_ATTR_PREFETCH_ROWS: ", err);
    }
    if (prefetchMem >= 0) {
        r = ACIAttrSet(sql,
            ACI_HTYPE_STMT,
            &prefetchMem,
            0,
            ACI_ATTR_PREFETCH_MEMORY,
            err);
        if (r != 0)
            qOraWarning("QACIResultPrivate::setStatementAttributes:"
                " Couldn't set ACI_ATTR_PREFETCH_MEMORY: ", err);
    }
}

int QACIResultPrivate::bindValue(ACIStmt *sql, ACIBind **hbnd, ACIError *err, int pos,
    const QVariant &val, dvoid *indPtr, ub2 *tmpSize, QList<QByteArray> &tmpStorage)
{
    int r = ACI_SUCCESS;
    void *data = const_cast<void *>(val.constData());

    switch (val.type()) {
    case QVariant::ByteArray:
        r = ACIBindByPos(sql, hbnd, err,
            pos + 1,
            isOutValue(pos)
            ? const_cast<char *>(reinterpret_cast<QByteArray *>(data)->constData())
            : reinterpret_cast<QByteArray *>(data)->data(),
            reinterpret_cast<QByteArray *>(data)->size(),
            SQLT_BIN, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
        break;
    case QVariant::Time:
    case QVariant::Date:
    case QVariant::DateTime: {
        QByteArray ba = qMakeOraDate(val.toDateTime());
        r = ACIBindByPos(sql, hbnd, err,
            pos + 1,
            ba.data(),
            ba.size(),
            SQLT_DAT, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
        tmpStorage.append(ba);
        break; }
    case QVariant::Int:
        r = ACIBindByPos(sql, hbnd, err,
            pos + 1,
            // if it's an out value, the data is already detached
            // so the const cast is safe.
            const_cast<void *>(data),
            sizeof(int),
            SQLT_INT, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
        break;
    case QVariant::UInt:
        r = ACIBindByPos(sql, hbnd, err,
            pos + 1,
            // if it's an out value, the data is already detached
            // so the const cast is safe.
            const_cast<void *>(data),
            sizeof(uint),
            SQLT_UIN, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
        break;
    case QVariant::LongLong:
    {
        QByteArray ba = qMakeACINumber(val.toLongLong(), err);
        r = ACIBindByPos(sql, hbnd, err,
            pos + 1,
            ba.data(),
            ba.size(),
            SQLT_VNU, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
        tmpStorage.append(ba);
        break;
    }
    case QVariant::ULongLong:
    {
        QByteArray ba = qMakeACINumber(val.toULongLong(), err);
        r = ACIBindByPos(sql, hbnd, err,
            pos + 1,
            ba.data(),
            ba.size(),
            SQLT_VNU, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
        tmpStorage.append(ba);
        break;
    }
    case QVariant::Double:
        r = ACIBindByPos(sql, hbnd, err,
            pos + 1,
            // if it's an out value, the data is already detached
            // so the const cast is safe.
            const_cast<void *>(data),
            sizeof(double),
            SQLT_FLT, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
        break;
    case QVariant::UserType:
        if (val.canConvert<QACIRowIdPointer>() && !isOutValue(pos)) {
            // use a const pointer to prevent a detach
            const QACIRowIdPointer rptr = qvariant_cast<QACIRowIdPointer>(val);
            r = ACIBindByPos(sql, hbnd, err,
                pos + 1,
                // it's an IN value, so const_cast is ok
                const_cast<ACIRowid **>(&rptr->id),
                -1,
                SQLT_RDD, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
        }
        else {
            qWarning("Unknown bind variable");
            r = ACI_ERROR;
        }
        break;
    case QVariant::String: {
        const QString s = val.toString();
        QByteArray utf8 = s.toUtf8();

        if (isBinaryValue(pos)) {
            r = ACIBindByPos(sql, hbnd, err,
                pos + 1,
                utf8.data(),
                utf8.size(),
                SQLT_LNG, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
            tmpStorage.append(utf8);
            break;
        }
        else if (!isOutValue(pos)) {
            // don't detach the string
            r = ACIBindByPos(sql, hbnd, err,
                pos + 1,
                // safe since oracle doesn't touch OUT values
                utf8.data(),
                utf8.size()+1,
                SQLT_STR, indPtr, 0, 0, 0, 0, ACI_DEFAULT);
            tmpStorage.append(utf8);
            if (r == ACI_SUCCESS)
                setCharset(*hbnd, ACI_HTYPE_BIND);
            break;
        }
    } // fall through for OUT values
    default: {
        const QString s = val.toString();
        // create a deep-copy
        QByteArray utf8 = s.toUtf8();
        QByteArray ba(utf8, utf8.size() + 1);
        if (isOutValue(pos)) {
            ba.reserve((s.capacity() + 1) * sizeof(QChar));
            *tmpSize = ba.size();
            r = ACIBindByPos(sql, hbnd, err,
                pos + 1,
                ba.data(),
                ba.capacity(),
                SQLT_STR, indPtr, tmpSize, 0, 0, 0, OCI_DEFAULT);
        }
        else {
            r = ACIBindByPos(sql, hbnd, err,
                pos + 1,
                ba.data(),
                ba.size(),
                SQLT_STR, indPtr, 0, 0, 0, 0, OCI_DEFAULT);
        }
        if (r == OCI_SUCCESS)
            setCharset(*hbnd, OCI_HTYPE_BIND);
        tmpStorage.append(ba);
        break;
    } // default case
    } // switch
    if (r != ACI_SUCCESS)
        qOraWarning("QACIResultPrivate::bindValue:", err);
    return r;
}

int QACIResultPrivate::bindValues(QVector<QVariant> &values, IndicatorArray &indicators,
    SizeArray &tmpSizes, QList<QByteArray> &tmpStorage)
{
    int r = ACI_SUCCESS;
    for (int i = 0; i < values.count(); ++i) {
        if (isOutValue(i))
            values[i].detach();
        const QVariant &val = values.at(i);

        ACIBind * hbnd = 0; // Oracle handles these automatically
        sb2 *indPtr = &indicators[i];
        *indPtr = val.isNull() ? -1 : 0;

        bindValue(sql, &hbnd, err, i, val, indPtr, &tmpSizes[i], tmpStorage);
    }
    return r;
}

// will assign out value and remove its temp storage.
static void qOraOutValue(QVariant &value, QList<QByteArray> &storage, ACIError* err)
{
    switch (value.type()) {
    case QVariant::Time:
        value = qMakeDate(storage.takeFirst()).time();
        break;
    case QVariant::Date:
        value = qMakeDate(storage.takeFirst()).date();
        break;
    case QVariant::DateTime:
        value = qMakeDate(storage.takeFirst());
        break;
    case QVariant::LongLong:
        value = qMakeLongLong(storage.takeFirst(), err);
        break;
    case QVariant::ULongLong:
        value = qMakeULongLong(storage.takeFirst(), err);
        break;
    case QVariant::String:
        /*value = QString(
            reinterpret_cast<const QChar *>(storage.takeFirst().constData()));*/
        // TODO zhaoj
        value = QString::fromUtf8(storage.takeFirst().constData());
        break;
    default:
        break; //nothing
    }
}

void QACIResultPrivate::outValues(QVector<QVariant> &values, IndicatorArray &indicators,
    QList<QByteArray> &tmpStorage)
{
    for (int i = 0; i < values.count(); ++i) {

        if (!isOutValue(i))
            continue;

        qOraOutValue(values[i], tmpStorage, err);

        QVariant::Type typ = values.at(i).type();
        if (indicators[i] == -1) // NULL
            values[i] = QVariant(typ);
        else
            values[i] = QVariant(typ, values.at(i).constData());
    }
}


class QACIDriverPrivate
{
public:
    QACIDriverPrivate();

    ACIEnv *env;
    ACISvcCtx *svc;
    ACIServer *srvhp;
    ACISession *authp;
    ACIError *err;
    bool transaction;
    int serverVersion;
    ub4 prefetchRows;
    ub2 prefetchMem;
    static QString user;

    void allocErrorHandle();
};
QString QACIDriverPrivate::user = "";
QACIDriverPrivate::QACIDriverPrivate()
    : env(0), svc(0), srvhp(0), authp(0), err(0), transaction(false),
    serverVersion(-1), prefetchRows(-1), prefetchMem(QACI_PREFETCH_MEM)
{

}

void QACIDriverPrivate::allocErrorHandle()
{
    int r = ACIHandleAlloc(env,
        reinterpret_cast<void **>(&err),
        ACI_HTYPE_ERROR,
        0,
        0);
    if (r != 0)
        qWarning("QACIDriver: unable to allocate error handle");
}

struct OraFieldInfo
{
    QString name;
    QVariant::Type type;
    ub1 oraIsNull;
    ub4 oraType;
    sb1 oraScale;
    ub4 oraLength; // size in bytes
    ub4 oraFieldLength; // amount of characters
    sb2 oraPrecision;
};

QString qOraWarn(ACIError *err, int *errorCode)
{
    sb4 errcode;
    text errbuf[1024];
    errbuf[0] = 0;
    errbuf[1] = 0;

    ACIErrorGet(err,
        1,
        0,
        &errcode,
        errbuf,
        sizeof(errbuf),
        ACI_HTYPE_ERROR);
    if (errorCode)
        *errorCode = errcode;
    return QString::fromLocal8Bit((const char*)errbuf);
}

void qOraWarning(const char* msg, ACIError *err)
{
#ifdef QACI_DEBUG
    qWarning("%s %s", msg, qUtf8Printable(qOraWarn(err)));
#else
    Q_UNUSED(msg);
    Q_UNUSED(err);
#endif
}

static int qOraErrorNumber(ACIError *err)
{
    sb4 errcode;
    ACIErrorGet(err,
        1,
        0,
        &errcode,
        0,
        0,
        ACI_HTYPE_ERROR);
    return errcode;
}

QSqlError qMakeError(const QString& errString, QSqlError::ErrorType type, ACIError *err)
{
    int errorCode = 0;
    const QString oraErrorString = qOraWarn(err, &errorCode);
    return QSqlError(errString, oraErrorString, type, errorCode);
}

QVariant::Type qDecodeACIType(const QString& ACItype, QSql::NumericalPrecisionPolicy precisionPolicy)
{
    QVariant::Type type = QVariant::Invalid;
    if (ACItype == QLatin1String("VARCHAR2") || ACItype == QLatin1String("VARCHAR")
        || ACItype.startsWith(QLatin1String("INTERVAL"))
        || ACItype == QLatin1String("CHAR") || ACItype == QLatin1String("NVARCHAR2")
        || ACItype == QLatin1String("NCHAR"))
        type = QVariant::String;
    else if (ACItype == QLatin1String("NUMBER")
        || ACItype == QLatin1String("FLOAT")
        || ACItype == QLatin1String("BINARY_FLOAT")
        || ACItype == QLatin1String("BINARY_DOUBLE")) {
        switch (precisionPolicy) {
        case QSql::LowPrecisionInt32:
            type = QVariant::Int;
            break;
        case QSql::LowPrecisionInt64:
            type = QVariant::LongLong;
            break;
        case QSql::LowPrecisionDouble:
            type = QVariant::Double;
            break;
        case QSql::HighPrecision:
        default:
            type = QVariant::String;
            break;
        }
    }
    else if (ACItype == QLatin1String("LONG") || ACItype == QLatin1String("NCLOB")
        || ACItype == QLatin1String("CLOB"))
        type = QVariant::ByteArray;
    else if (ACItype == QLatin1String("RAW") || ACItype == QLatin1String("LONG RAW")
        || ACItype == QLatin1String("ROWID") || ACItype == QLatin1String("BLOB")
        || ACItype == QLatin1String("CFILE") || ACItype == QLatin1String("BFILE"))
        type = QVariant::ByteArray;
    else if (ACItype == QLatin1String("DATE") || ACItype.startsWith(QLatin1String("TIME")))
        type = QVariant::DateTime;
    else if (ACItype == QLatin1String("UNDEFINED"))
        type = QVariant::Invalid;
    if (type == QVariant::Invalid)
        qWarning("qDecodeACIType: unknown type: %s", ACItype.toLocal8Bit().constData());
    return type;
}

QVariant::Type qDecodeACIType(int ACItype, QSql::NumericalPrecisionPolicy precisionPolicy)
{
    QVariant::Type type = QVariant::Invalid;
    switch (ACItype) {
    case SQLT_STR:
    case SQLT_VST:
    case SQLT_CHR:
    case SQLT_AFC:
    case SQLT_VCS:
    case SQLT_AVC:
    case SQLT_RDD:
    case SQLT_LNG:
#ifdef SQLT_INTERVAL_YM
    case SQLT_INTERVAL_YM:
#endif
#ifdef SQLT_INTERVAL_DS
    case SQLT_INTERVAL_DS:
#endif
        type = QVariant::String;
        break;
    case SQLT_INT:
        type = QVariant::Int;
        break;
    case SQLT_FLT:
    case SQLT_NUM:
    case SQLT_VNU:
    case SQLT_UIN:
        switch (precisionPolicy) {
        case QSql::LowPrecisionInt32:
            type = QVariant::Int;
            break;
        case QSql::LowPrecisionInt64:
            type = QVariant::LongLong;
            break;
        case QSql::LowPrecisionDouble:
            type = QVariant::Double;
            break;
        case QSql::HighPrecision:
        default:
            type = QVariant::String;
            break;
        }
        break;
    case SQLT_VBI:
    case SQLT_BIN:
    case SQLT_LBI:
    case SQLT_LVC:
    case SQLT_LVB:
    case SQLT_BLOB:
    case SQLT_CLOB:
    case SQLT_FILE:
    case SQLT_NTY:
    case SQLT_REF:
    case SQLT_RID:
        type = QVariant::ByteArray;
        break;
    case SQLT_DAT:
    case SQLT_ODT:
#ifdef SQLT_TIMESTAMP
    case SQLT_TIMESTAMP:
    case SQLT_TIMESTAMP_TZ:
    case SQLT_TIMESTAMP_LTZ:
#endif
        type = QVariant::DateTime;
        break;
    default:
        type = QVariant::Invalid;
        qWarning("qDecodeACIType: unknown ACI datatype: %d", ACItype);
        break;
    }
    return type;
}

static QSqlField qFromOraInf(const OraFieldInfo &ofi)
{
    QSqlField f(ofi.name, ofi.type);
    f.setRequired(ofi.oraIsNull == 0);

    if (ofi.type == QVariant::String && ofi.oraType != SQLT_NUM && ofi.oraType != SQLT_VNU)
        f.setLength(ofi.oraFieldLength);
    else
        f.setLength(ofi.oraPrecision == 0 ? 38 : int(ofi.oraPrecision));

    f.setPrecision(ofi.oraScale);
    f.setSqlType(int(ofi.oraType));
    return f;
}

/*!
\internal

Convert QDateTime to the internal Oracle DATE format NB!
It does not handle BCE dates.
*/
QByteArray qMakeOraDate(const QDateTime& dt)
{
    QByteArray ba;
    ba.resize(7);
    int year = dt.date().year();
    ba[0] = (year / 100) + 100; // century
    ba[1] = (year % 100) + 100; // year
    ba[2] = dt.date().month();
    ba[3] = dt.date().day();
    ba[4] = dt.time().hour() + 1;
    ba[5] = dt.time().minute() + 1;
    ba[6] = dt.time().second() + 1;
    return ba;
}

/*!
\internal

Convert qlonglong to the internal Oracle ACINumber format.
*/
QByteArray qMakeACINumber(const qlonglong& ll, ACIError* err)
{
    QByteArray ba(sizeof(ACINumber), 0);

    ACINumberFromInt(err,
        &ll,
        sizeof(qlonglong),
        ACI_NUMBER_SIGNED,
        reinterpret_cast<ACINumber*>(ba.data()));
    return ba;
}

/*!
\internal

Convert qulonglong to the internal Oracle ACINumber format.
*/
QByteArray qMakeACINumber(const qulonglong& ull, ACIError* err)
{
    QByteArray ba(sizeof(ACINumber), 0);

    ACINumberFromInt(err,
        &ull,
        sizeof(qlonglong),
        ACI_NUMBER_UNSIGNED,
        reinterpret_cast<ACINumber*>(ba.data()));
    return ba;
}

qlonglong qMakeLongLong(const char* acinummber, ACIError* err)
{
    qlonglong qll = 0;
    ACINumberToInt(err, reinterpret_cast<const ACINumber *>(acinummber), sizeof(qlonglong),
        ACI_NUMBER_SIGNED, &qll);
    return qll;
}

qulonglong qMakeULongLong(const char* acinummber, ACIError* err)
{
    qulonglong qull = 0;
    ACINumberToInt(err, reinterpret_cast<const ACINumber *>(acinummber), sizeof(qulonglong),
        ACI_NUMBER_UNSIGNED, &qull);
    return qull;
}

QDateTime qMakeDate(const char* oraDate)
{
    qDebug() << strlen(oraDate);
    int century = uchar(oraDate[0]);
    if (century >= 100) {
        int year = uchar(oraDate[1]);
        year = ((century - 100) * 100) + (year - 100);
        int month = oraDate[2];
        int day = oraDate[3];
        int hour = oraDate[4] - 1;
        int min = oraDate[5] - 1;
        int sec = oraDate[6] - 1;
        return QDateTime(QDate(year, month, day), QTime(hour, min, sec));
    }
    return QDateTime();
}

class QACICols
{
public:
    QACICols(int size, QACIResultPrivate* dp);
    ~QACICols();
    int readPiecewise(QVector<QVariant> &values, int index = 0);
    int readLOBs(QVector<QVariant> &values, int index = 0);
    int fieldFromDefine(ACIDefine* d);
    void getValues(QVector<QVariant> &v, int index);
    inline int size() { return fieldInf.size(); }
    static bool execBatch(QACIResultPrivate *d, QVector<QVariant> &boundValues, bool arrayBind);

    QSqlRecord rec;

private:
    char* create(int position, int size);
    ACILobLocator ** createLobLocator(int position, ACIEnv* env);
    OraFieldInfo qMakeOraField(const QACIResultPrivate* p, ACIParam* param) const;

    class OraFieldInf
    {
    public:
        OraFieldInf() : data(0), len(0), ind(0), typ(QVariant::Invalid), oraType(0), def(0), lob(0)
        {}
        ~OraFieldInf();
        char *data;
        int len;
        sb2 ind;
        QVariant::Type typ;
        ub4 oraType;
        ACIDefine *def;
        ACILobLocator *lob;
    };

    QVector<OraFieldInf> fieldInf;
    const QACIResultPrivate *const d;
};

QACICols::OraFieldInf::~OraFieldInf()
{
    delete[] data;
    data = nullptr;
    if (lob) {
        int r = ACIDescriptorFree(lob, ACI_DTYPE_LOB);
        if (r != 0)
            qWarning("QACICols: Cannot free LOB descriptor");
    }
}

QACICols::QACICols(int size, QACIResultPrivate* dp)
    : fieldInf(size), d(dp)
{
    ub4 dataSize = 0;
    ACIDefine* dfn = 0;
    int r;

    ACIParam* param = 0;
    sb4 parmStatus = 0;
    ub4 count = 1;
    int idx = 0;
    parmStatus = ACIParamGet(d->sql,
        ACI_HTYPE_STMT,
        d->err,
        reinterpret_cast<void **>(&param),
        count);

    while (parmStatus == ACI_SUCCESS) {
        OraFieldInfo ofi = qMakeOraField(d, param);
        if (ofi.oraType == SQLT_RDD)
            dataSize = 50;
#ifdef SQLT_INTERVAL_YM
#ifdef SQLT_INTERVAL_DS
        else if (ofi.oraType == SQLT_INTERVAL_YM || ofi.oraType == SQLT_INTERVAL_DS)
            // since we are binding interval datatype as string,
            // we are not interested in the number of bytes but characters.
            dataSize = 50;  // magic number
#endif //SQLT_INTERVAL_DS
#endif //SQLT_INTERVAL_YM
        else if (ofi.oraType == SQLT_NUM || ofi.oraType == SQLT_VNU) {
            if (ofi.oraPrecision > 0)
                dataSize = (ofi.oraPrecision + 1) * sizeof(utext);
            else
                dataSize = (38 + 1) * sizeof(utext);
        }
        else
            dataSize = ofi.oraLength;

        fieldInf[idx].typ = ofi.type;
        fieldInf[idx].oraType = ofi.oraType;
        rec.append(qFromOraInf(ofi));

        switch (ofi.type) {
        case QVariant::DateTime:
            r = ACIDefineByPos(d->sql,
                &dfn,
                d->err,
                count,
                create(idx, dataSize + 1),
                dataSize + 1,
                SQLT_DAT,
                &(fieldInf[idx].ind),
                0, 0, ACI_DEFAULT);
            break;
        case QVariant::Double:
            r = ACIDefineByPos(d->sql,
                &dfn,
                d->err,
                count,
                create(idx, sizeof(double) - 1),
                sizeof(double),
                SQLT_FLT,
                &(fieldInf[idx].ind),
                0, 0, ACI_DEFAULT);
            break;
        case QVariant::Int:
            r = ACIDefineByPos(d->sql,
                &dfn,
                d->err,
                count,
                create(idx, sizeof(qint32) - 1),
                sizeof(qint32),
                SQLT_INT,
                &(fieldInf[idx].ind),
                0, 0, ACI_DEFAULT);
            break;
        case QVariant::LongLong:
            r = ACIDefineByPos(d->sql,
                &dfn,
                d->err,
                count,
                create(idx, sizeof(ACINumber)),
                sizeof(ACINumber),
                SQLT_VNU,
                &(fieldInf[idx].ind),
                0, 0, ACI_DEFAULT);
            break;
        case QVariant::ByteArray:
            // RAW and LONG RAW fields can't be bound to LOB locators
            if (ofi.oraType == SQLT_BIN) {
                //                                qDebug("binding SQLT_BIN");
                r = ACIDefineByPos(d->sql,
                    &dfn,
                    d->err,
                    count,
                    create(idx, dataSize),
                    dataSize,
                    SQLT_BIN,
                    &(fieldInf[idx].ind),
                    0, 0, ACI_DYNAMIC_FETCH);
            }
            else if (ofi.oraType == SQLT_LBI) {
                //                                    qDebug("binding SQLT_LBI");
                r = ACIDefineByPos(d->sql,
                    &dfn,
                    d->err,
                    count,
                    0,
                    SB4MAXVAL,
                    SQLT_LBI,
                    &(fieldInf[idx].ind),
                    0, 0, ACI_DYNAMIC_FETCH);
            }
            else if (ofi.oraType == SQLT_CLOB) {
                r = ACIDefineByPos(d->sql,
                    &dfn,
                    d->err,
                    count,
                    createLobLocator(idx, d->env),
                    -1,
                    SQLT_CLOB,
                    &(fieldInf[idx].ind),
                    0, 0, ACI_DEFAULT);
            }
            else {
                //                 qDebug("binding SQLT_BLOB");
                r = ACIDefineByPos(d->sql,
                    &dfn,
                    d->err,
                    count,
                    createLobLocator(idx, d->env),
                    -1,
                    SQLT_BLOB,
                    &(fieldInf[idx].ind),
                    0, 0, ACI_DEFAULT);
            }
            break;
        case QVariant::String:
            if (ofi.oraType == SQLT_LNG) {
                r = ACIDefineByPos(d->sql,
                    &dfn,
                    d->err,
                    count,
                    0,
                    SB4MAXVAL,
                    SQLT_LNG,
                    &(fieldInf[idx].ind),
                    0, 0, ACI_DYNAMIC_FETCH);
            }
            else {
                dataSize += dataSize + sizeof(char);
                // TODO zhaoj
                //qDebug("ACIDefineByPosStr(%d): %d", count, dataSize);
                r = ACIDefineByPos(d->sql,
                    &dfn,
                    d->err,
                    count,
                    create(idx, dataSize),
                    dataSize,
                    SQLT_STR,
                    &(fieldInf[idx].ind),
                    0, 0, ACI_DEFAULT);
                if (r == 0)
                    d->setCharset(dfn, ACI_HTYPE_DEFINE);
            }
            break;
        default:
            // this should make enough space even with character encoding
            dataSize = (dataSize + 1) * sizeof(utext);
            //qDebug("ACIDefineByPosDef(%d): %d", count, dataSize);
            r = ACIDefineByPos(d->sql,
                &dfn,
                d->err,
                count,
                create(idx, dataSize),
                dataSize + 1,
                SQLT_STR,
                &(fieldInf[idx].ind),
                0, 0, ACI_DEFAULT);
            break;
        }
        if (r != 0)
            qOraWarning("QACICols::bind:", d->err);
        fieldInf[idx].def = dfn;
        ++count;
        ++idx;
        parmStatus = ACIParamGet(d->sql,
            ACI_HTYPE_STMT,
            d->err,
            reinterpret_cast<void **>(&param),
            count);
    }
}

QACICols::~QACICols()
{
}

char* QACICols::create(int position, int size)
{
    char* c = new char[size + 1];
    // Oracle may not fill fixed width fields
    memset(c, 0, size + 1);
    fieldInf[position].data = c;
    fieldInf[position].len = size;
    return c;
}

ACILobLocator **QACICols::createLobLocator(int position, ACIEnv* env)
{
    ACILobLocator *& lob = fieldInf[position].lob;
    int r = ACIDescriptorAlloc(env,
        reinterpret_cast<void **>(&lob),
        ACI_DTYPE_LOB,
        0,
        0);
    if (r != 0) {
        qWarning("QACICols: Cannot create LOB locator");
        lob = 0;
    }
    return &lob;
}

int QACICols::readPiecewise(QVector<QVariant> &values, int index)
{
    ACIDefine*     dfn;
    ub4            typep;
    ub1            in_outp;
    ub4            iterp;
    ub4            idxp;
    ub1            piecep;
    sword          status;
    text           col[QACI_DYNAMIC_CHUNK_SIZE + 1];
    int            fieldNum = -1;
    int            r = 0;
    bool           nullField;

    do {
        r = ACIStmtGetPieceInfo(d->sql, d->err, reinterpret_cast<void **>(&dfn), &typep,
            &in_outp, &iterp, &idxp, &piecep);
        if (r != ACI_SUCCESS)
            qOraWarning("ACIResultPrivate::readPiecewise: unable to get piece info:", d->err);
        fieldNum = fieldFromDefine(dfn);
        bool isStringField = fieldInf.at(fieldNum).oraType == SQLT_LNG;
        ub4 chunkSize = QACI_DYNAMIC_CHUNK_SIZE;
        nullField = false;
        r = ACIStmtSetPieceInfo(dfn, ACI_HTYPE_DEFINE,
            d->err, col,
            &chunkSize, piecep, NULL, NULL);
        if (r != ACI_SUCCESS)
            qOraWarning("ACIResultPrivate::readPiecewise: unable to set piece info:", d->err);
        status = ACIStmtFetch(d->sql, d->err, 1, ACI_FETCH_NEXT, ACI_DEFAULT);
        if (status == -1) {
            sb4 errcode;
            ACIErrorGet(d->err, 1, 0, &errcode, 0, 0, ACI_HTYPE_ERROR);
            switch (errcode) {
            case 1405: /* NULL */
                nullField = true;
                break;
            default:
                qOraWarning("ACIResultPrivate::readPiecewise: unable to fetch next:", d->err);
                break;
            }
        }
        if (status == ACI_NO_DATA)
            break;
        if (nullField || !chunkSize) {
            fieldInf[fieldNum].ind = -1;
        }
        else {
            if (isStringField) {
                QString str = values.at(fieldNum + index).toString();
                str += QString::fromUtf8((char*)col);
                values[fieldNum + index] = str;
                fieldInf[fieldNum].ind = 0;
            }
            else {
                QByteArray ba = values.at(fieldNum + index).toByteArray();
                int sz = ba.size();
                ba.resize(sz + chunkSize);
                memcpy(ba.data() + sz, reinterpret_cast<char *>(col), chunkSize);
                values[fieldNum + index] = ba;
                fieldInf[fieldNum].ind = 0;
            }
        }
    } while (status == ACI_SUCCESS_WITH_INFO || status == ACI_NEED_DATA);
    return r;
}

OraFieldInfo QACICols::qMakeOraField(const QACIResultPrivate* p, ACIParam* param) const
{
    OraFieldInfo ofi;
    ub2 colType(0);
    text *colName = 0;
    ub4 colNameLen(0);
    sb1 colScale(0);
    ub2 colLength(0);
    ub2 colFieldLength(0);
    sb2 colPrecision(0);
    ub1 colIsNull(0);
    int r(0);
    QVariant::Type type(QVariant::Invalid);

    r = ACIAttrGet(param,
        ACI_DTYPE_PARAM,
        &colType,
        0,
        ACI_ATTR_DATA_TYPE,
        p->err);
    if (r != 0)
        qOraWarning("qMakeOraField:", p->err);

    r = ACIAttrGet(param,
        ACI_DTYPE_PARAM,
        &colName,
        &colNameLen,
        ACI_ATTR_NAME,
        p->err);
    if (r != 0)
        qOraWarning("qMakeOraField:", p->err);

    r = ACIAttrGet(param,
        ACI_DTYPE_PARAM,
        &colLength,
        0,
        ACI_ATTR_DATA_SIZE, /* in bytes */
        p->err);
    if (r != 0)
        qOraWarning("qMakeOraField:", p->err);

//#ifdef ACI_ATTR_CHAR_USED
//    ub1 char_semantics = 0;
//    
//    r=ACIAttrGet(param, 
//        ACI_DTYPE_PARAM,
//        &char_semantics, 
//        0, 
//        ACI_ATTR_CHAR_USED,
//        p->err);
//
//    if(r!=ACI_SUCCESS)
//        qOraWarning("qMakeOraField:", p->err);
//    if (char_semantics) {
//        r = ACIAttrGet(param,
//            ACI_DTYPE_PARAM,
//            &colFieldLength,
//            0,
//            ACI_ATTR_CHAR_SIZE,
//            p->err);
//        if (r != 0)
//            qOraWarning("qMakeOraField:", p->err);
//    }
//#else
//    colFieldLength = colLength;
//#endif
    // TODO zhaoj
    colFieldLength = colLength;

    r = ACIAttrGet(param,
        ACI_DTYPE_PARAM,
        &colPrecision,
        0,
        ACI_ATTR_PRECISION,
        p->err);
    if (r != 0)
        qOraWarning("qMakeOraField:", p->err);

    r = ACIAttrGet(param,
        ACI_DTYPE_PARAM,
        &colScale,
        0,
        ACI_ATTR_SCALE,
        p->err);
    if (r != 0)
        qOraWarning("qMakeOraField:", p->err);
    r = ACIAttrGet(param,
        ACI_DTYPE_PARAM,
        &colType,
        0,
        ACI_ATTR_DATA_TYPE,
        p->err);
    if (r != 0)
        qOraWarning("qMakeOraField:", p->err);
    r = ACIAttrGet(param,
        ACI_DTYPE_PARAM,
        &colIsNull,
        0,
        ACI_ATTR_IS_NULL,
        p->err);
    if (r != 0)
        qOraWarning("qMakeOraField:", p->err);

    type = qDecodeACIType(colType, p->q->numericalPrecisionPolicy());

    if (type == QVariant::Int) {
        if (colLength == 22 && colPrecision == 0 && colScale == 0)
            type = QVariant::String;
        if (colScale > 0)
            type = QVariant::String;
    }

    // bind as double if the precision policy asks for it
    if (((colType == SQLT_FLT) || (colType == SQLT_NUM))
        && (p->q->numericalPrecisionPolicy() == QSql::LowPrecisionDouble)) {
        type = QVariant::Double;
    }

    // bind as int32 or int64 if the precision policy asks for it
    if ((colType == SQLT_NUM) || (colType == SQLT_VNU) || (colType == SQLT_UIN)
        || (colType == SQLT_INT)) {
        if (p->q->numericalPrecisionPolicy() == QSql::LowPrecisionInt64)
            type = QVariant::LongLong;
        else if (p->q->numericalPrecisionPolicy() == QSql::LowPrecisionInt32)
            type = QVariant::Int;
    }

    if (colType == SQLT_BLOB)
        colLength = 0;

    // colNameLen is length in bytes
    ofi.name = QString::fromUtf8((char*)colName);
    ofi.type = type;
    ofi.oraType = colType;
    ofi.oraFieldLength = colFieldLength;
    ofi.oraLength = colLength;
    ofi.oraScale = colScale;
    ofi.oraPrecision = colPrecision;
    ofi.oraIsNull = colIsNull;

    return ofi;
}

struct QACIBatchColumn
{
    inline QACIBatchColumn()
        : bindh(0), bindAs(0), maxLen(0), recordCount(0),
        data(0), lengths(0), indicators(0), maxarr_len(0), curelep(0) {}

    ACIBind* bindh;
    ub2 bindAs;
    ub4 maxLen;
    ub4 recordCount;
    char* data;
    ub2* lengths;
    sb2* indicators;
    ub4 maxarr_len;
    ub4 curelep;
};

struct QACIBatchCleanupHandler
{
    inline QACIBatchCleanupHandler(QVector<QACIBatchColumn> &columns)
        : col(columns) {}

    ~QACIBatchCleanupHandler()
    {
        // deleting storage, length and indicator arrays
        for (int j = 0; j < col.count(); ++j) {
            delete[] col[j].lengths;
            delete[] col[j].indicators;
            delete[] col[j].data;
        }
    }

    QVector<QACIBatchColumn> &col;
};

bool QACICols::execBatch(QACIResultPrivate *d, QVector<QVariant> &boundValues, bool arrayBind)
{
    int columnCount = boundValues.count();
    if (boundValues.isEmpty() || columnCount == 0)
        return false;

#ifdef QACI_DEBUG
    qDebug() << "columnCount:" << columnCount << boundValues;
#endif

    int i;
    sword r;

    QVarLengthArray<QVariant::Type> fieldTypes;
    for (i = 0; i < columnCount; ++i) {
        QVariant::Type tp = boundValues.at(i).type();
        fieldTypes.append(tp == QVariant::List ? boundValues.at(i).toList().value(0).type()
            : tp);
    }

    QList<QByteArray> tmpStorage;
    SizeArray tmpSizes(columnCount);
    QVector<QACIBatchColumn> columns(columnCount);
    QACIBatchCleanupHandler cleaner(columns);

    // figuring out buffer sizes
    for (i = 0; i < columnCount; ++i) {

        if (boundValues.at(i).type() != QVariant::List) {

            // not a list - create a deep-copy of the single value
            QACIBatchColumn &singleCol = columns[i];
            singleCol.indicators = new sb2[1];
            *singleCol.indicators = boundValues.at(i).isNull() ? -1 : 0;

            r = d->bindValue(d->sql, &singleCol.bindh, d->err, i,
                boundValues.at(i), singleCol.indicators, &tmpSizes[i], tmpStorage);

            if (r != ACI_SUCCESS && r != ACI_SUCCESS_WITH_INFO) {
                qOraWarning("QACIPrivate::execBatch: unable to bind column:", d->err);
                d->q->setLastError(qMakeError(QCoreApplication::translate("QACIResult",
                    "Unable to bind column for batch execute"),
                    QSqlError::StatementError, d->err));
                return false;
            }
            continue;
        }

        QACIBatchColumn &col = columns[i];
        col.recordCount = boundValues.at(i).toList().count();

        col.lengths = new ub2[col.recordCount];
        col.indicators = new sb2[col.recordCount];
        col.maxarr_len = col.recordCount;
        col.curelep = col.recordCount;

        switch (fieldTypes[i]) {
        case QVariant::Time:
        case QVariant::Date:
        case QVariant::DateTime:
            col.bindAs = SQLT_DAT;
            col.maxLen = 7;
            break;

        case QVariant::Int:
            col.bindAs = SQLT_INT;
            col.maxLen = sizeof(int);
            break;

        case QVariant::UInt:
            col.bindAs = SQLT_UIN;
            col.maxLen = sizeof(uint);
            break;

        case QVariant::LongLong:
            col.bindAs = SQLT_VNU;
            col.maxLen = sizeof(ACINumber);
            break;

        case QVariant::ULongLong:
            col.bindAs = SQLT_VNU;
            col.maxLen = sizeof(ACINumber);
            break;

        case QVariant::Double:
            col.bindAs = SQLT_FLT;
            col.maxLen = sizeof(double);
            break;

        case QVariant::UserType:
            col.bindAs = SQLT_RDD;
            col.maxLen = sizeof(ACIRowid*);
            break;

        case QVariant::String: {
            col.bindAs = SQLT_STR;
            for (uint j = 0; j < col.recordCount; ++j) {
                uint len;
                if (d->isOutValue(i))
                    len = boundValues.at(i).toList().at(j).toString().capacity() + 1;
                else
                    len = boundValues.at(i).toList().at(j).toString().length() + 1;
                if (len > col.maxLen)
                    col.maxLen = len;
            }
            col.maxLen *= sizeof(QChar);
            break; }

        case QVariant::ByteArray:
        default: {
            col.bindAs = SQLT_LBI;
            for (uint j = 0; j < col.recordCount; ++j) {
                if (d->isOutValue(i))
                    col.lengths[j] = boundValues.at(i).toList().at(j).toByteArray().capacity();
                else
                    col.lengths[j] = boundValues.at(i).toList().at(j).toByteArray().size();
                if (col.lengths[j] > col.maxLen)
                    col.maxLen = col.lengths[j];
            }
            break; }
        }

        col.data = new char[col.maxLen * col.recordCount];
        memset(col.data, 0, col.maxLen * col.recordCount);

        // we may now populate column with data
        for (uint row = 0; row < col.recordCount; ++row) {
            const QVariant &val = boundValues.at(i).toList().at(row);

            if (val.isNull()) {
                columns[i].indicators[row] = -1;
                columns[i].lengths[row] = 0;
            }
            else {
                columns[i].indicators[row] = 0;
                char *dataPtr = columns[i].data + (columns[i].maxLen * row);
                switch (fieldTypes[i]) {
                case QVariant::Time:
                case QVariant::Date:
                case QVariant::DateTime: {
                    columns[i].lengths[row] = columns[i].maxLen;
                    const QByteArray ba = qMakeOraDate(val.toDateTime());
                    Q_ASSERT(ba.size() == int(columns[i].maxLen));
                    memcpy(dataPtr, ba.constData(), columns[i].maxLen);
                    break;
                }
                case QVariant::Int:
                    columns[i].lengths[row] = columns[i].maxLen;
                    *reinterpret_cast<int*>(dataPtr) = val.toInt();
                    break;

                case QVariant::UInt:
                    columns[i].lengths[row] = columns[i].maxLen;
                    *reinterpret_cast<uint*>(dataPtr) = val.toUInt();
                    break;

                case QVariant::LongLong:
                {
                    columns[i].lengths[row] = columns[i].maxLen;
                    const QByteArray ba = qMakeACINumber(val.toLongLong(), d->err);
                    Q_ASSERT(ba.size() == int(columns[i].maxLen));
                    memcpy(dataPtr, ba.constData(), columns[i].maxLen);
                    break;
                }
                case QVariant::ULongLong:
                {
                    columns[i].lengths[row] = columns[i].maxLen;
                    const QByteArray ba = qMakeACINumber(val.toULongLong(), d->err);
                    Q_ASSERT(ba.size() == int(columns[i].maxLen));
                    memcpy(dataPtr, ba.constData(), columns[i].maxLen);
                    break;
                }
                case QVariant::Double:
                    columns[i].lengths[row] = columns[i].maxLen;
                    *reinterpret_cast<double*>(dataPtr) = val.toDouble();
                    break;

                case QVariant::String: {
                    const QString s = val.toString();
                    QByteArray ba = s.toUtf8();
                    columns[i].lengths[row] = ba.length();
                    memcpy(dataPtr, ba.constData(), ba.length());
                    break;
                }
                case QVariant::UserType:
                    if (val.canConvert<QACIRowIdPointer>()) {
                        const QACIRowIdPointer rptr = qvariant_cast<QACIRowIdPointer>(val);
                        *reinterpret_cast<ACIRowid**>(dataPtr) = rptr->id;
                        columns[i].lengths[row] = 0;
                        break;
                    }
                case QVariant::ByteArray:
                default: {
                    const QByteArray ba = val.toByteArray();
                    columns[i].lengths[row] = ba.size();
                    memcpy(dataPtr, ba.constData(), ba.size());
                    break;
                }
                }
            }
        }

        QACIBatchColumn &bindColumn = columns[i];

#ifdef QACI_DEBUG
        qDebug("ACIBindByPos(%p, %p, %p, %d, %p, %d, %d, %p, %p, 0, %d, %p, ACI_DEFAULT)",
            d->sql, &bindColumn.bindh, d->err, i + 1, bindColumn.data,
            bindColumn.maxLen, bindColumn.bindAs, bindColumn.indicators, bindColumn.lengths,
            arrayBind ? bindColumn.maxarr_len : 0, arrayBind ? &bindColumn.curelep : 0);

        for (int ii = 0; ii < (int)bindColumn.recordCount; ++ii) {
            qDebug(" record %d: indicator %d, length %d", ii, bindColumn.indicators[ii],
                bindColumn.lengths[ii]);
        }
#endif


        // binding the column
        r = ACIBindByPos(
            d->sql, &bindColumn.bindh, d->err, i + 1,
            bindColumn.data,
            bindColumn.maxLen,
            bindColumn.bindAs,
            bindColumn.indicators,
            bindColumn.lengths,
            0,
            arrayBind ? bindColumn.maxarr_len : 0,
            arrayBind ? &bindColumn.curelep : 0,
            ACI_DEFAULT);

#ifdef QACI_DEBUG
        qDebug("After ACIBindByPos: r = %d, bindh = %p", r, bindColumn.bindh);
#endif

        if (r != ACI_SUCCESS && r != ACI_SUCCESS_WITH_INFO) {
            qOraWarning("QACIPrivate::execBatch: unable to bind column:", d->err);
            d->q->setLastError(qMakeError(QCoreApplication::translate("QACIResult",
                "Unable to bind column for batch execute"),
                QSqlError::StatementError, d->err));
            return false;
        }

        r = ACIBindArrayOfStruct(
            columns[i].bindh, d->err,
            columns[i].maxLen,
            sizeof(columns[i].indicators[0]),
            sizeof(columns[i].lengths[0]),
            0);

        if (r != ACI_SUCCESS && r != ACI_SUCCESS_WITH_INFO) {
            qOraWarning("QACIPrivate::execBatch: unable to bind column:", d->err);
            d->q->setLastError(qMakeError(QCoreApplication::translate("QACIResult",
                "Unable to bind column for batch execute"),
                QSqlError::StatementError, d->err));
            return false;
        }
    }

    //finaly we can execute
    r = ACIStmtExecute(d->svc, d->sql, d->err,
        arrayBind ? 1 : columns[0].recordCount,
        0, NULL, NULL,
        d->transaction ? ACI_DEFAULT : ACI_COMMIT_ON_SUCCESS);

    if (r != ACI_SUCCESS && r != ACI_SUCCESS_WITH_INFO) {
        qOraWarning("QACIPrivate::execBatch: unable to execute batch statement:", d->err);
        d->q->setLastError(qMakeError(QCoreApplication::translate("QACIResult",
            "Unable to execute batch statement"),
            QSqlError::StatementError, d->err));
        return false;
    }

    // for out parameters we copy data back to value vector
    for (i = 0; i < columnCount; ++i) {

        if (!d->isOutValue(i))
            continue;

        QVariant::Type tp = boundValues.at(i).type();
        if (tp != QVariant::List) {
            qOraOutValue(boundValues[i], tmpStorage, d->err);
            if (*columns[i].indicators == -1)
                boundValues[i] = QVariant(tp);
            continue;
        }

        QVariantList *list = static_cast<QVariantList *>(const_cast<void*>(boundValues.at(i).data()));

        char* data = columns[i].data;
        for (uint r = 0; r < columns[i].recordCount; ++r) {

            if (columns[i].indicators[r] == -1) {
                (*list)[r] = QVariant();
                continue;
            }

            switch (columns[i].bindAs) {

            case SQLT_DAT:
                (*list)[r] = qMakeDate(data + r * columns[i].maxLen);
                break;

            case SQLT_INT:
                (*list)[r] = *reinterpret_cast<int*>(data + r * columns[i].maxLen);
                break;

            case SQLT_UIN:
                (*list)[r] = *reinterpret_cast<uint*>(data + r * columns[i].maxLen);
                break;

            case SQLT_VNU:
            {
                switch (boundValues.at(i).type()) {
                case QVariant::LongLong:
                    (*list)[r] = qMakeLongLong(data + r * columns[i].maxLen, d->err);
                    break;
                case QVariant::ULongLong:
                    (*list)[r] = qMakeULongLong(data + r * columns[i].maxLen, d->err);
                    break;
                default:
                    break;
                }
                break;
            }

            case SQLT_FLT:
                (*list)[r] = *reinterpret_cast<double*>(data + r * columns[i].maxLen);
                break;

            case SQLT_STR:
                (*list)[r] = QString::fromUtf8(reinterpret_cast<char*>(data));
                break;

            default:
                (*list)[r] = QByteArray(data + r * columns[i].maxLen, columns[i].maxLen);
                break;
            }
        }
    }

    d->q->setSelect(false);
    d->q->setAt(QSql::BeforeFirstRow);
    d->q->setActive(true);

    return true;
}

template<class T, int sz>
int qReadLob(T &buf, const QACIResultPrivate *d, ACILobLocator *lob)
{
    ub1 csfrm;
    ub4 amount;
    int r;

    // Read this from the database, don't assume we know what it is set to
    r = ACILobCharSetForm(d->env, d->err, lob, &csfrm);
    if (r != ACI_SUCCESS) {
        qOraWarning("ACIResultPrivate::readLobs: Couldn't get LOB char set form: ", d->err);
        csfrm = 0;
    }

    // Get the length of the LOB (this is in characters)
    r = ACILobGetLength(d->svc, d->err, lob, &amount);
    if (r == ACI_SUCCESS) {
        if (amount == 0) {
            // Short cut for null LOBs
            buf.resize(0);
            return ACI_SUCCESS;
        }
    }
    else {
        qOraWarning("ACIResultPrivate::readLobs: Couldn't get LOB length: ", d->err);
        return r;
    }

    // Resize the buffer to hold the LOB contents
    buf.resize(amount);

    // Read the LOB into the buffer
    r = ACILobRead(d->svc,
        d->err,
        lob,
        &amount,
        1,
        buf.data(),
        buf.size() * sz, // this argument is in bytes, not characters
        0,
        0,
        //// Extract the data from a CLOB in UTF-16 (ie. what QString uses internally)
        //sz == 1 ? ub2(0) : ub2(QACIEncoding),
        0,
        csfrm);

    if (r != ACI_SUCCESS)
        qOraWarning("ACIResultPrivate::readLOBs: Cannot read LOB: ", d->err);

    return r;
}

int QACICols::readLOBs(QVector<QVariant> &values, int index)
{
    ACILobLocator *lob;
    int r = ACI_SUCCESS;

    for (int i = 0; i < size(); ++i) {
        const OraFieldInf &fi = fieldInf.at(i);
        if (fi.ind == -1 || !(lob = fi.lob))
            continue;

        bool isClob = fi.oraType == SQLT_CLOB;
        QVariant var;

        QByteArray buf;
        r = qReadLob<QByteArray, sizeof(char)>(buf, d, lob);
        var = buf;

        if (r == ACI_SUCCESS)
            values[index + i] = var;
        else
            break;
    }
    return r;
}

int QACICols::fieldFromDefine(ACIDefine* d)
{
    for (int i = 0; i < fieldInf.count(); ++i) {
        if (fieldInf.at(i).def == d)
            return i;
    }
    return -1;
}

void QACICols::getValues(QVector<QVariant> &v, int index)
{
    for (int i = 0; i < fieldInf.size(); ++i) {
        const OraFieldInf &fld = fieldInf.at(i);

        if (fld.ind == -1) {
            // got a NULL value
            v[index + i] = QVariant(fld.typ);
            continue;
        }

        if (fld.oraType == SQLT_BIN || fld.oraType == SQLT_LBI || fld.oraType == SQLT_LNG)
            continue; // already fetched piecewise

        switch (fld.typ) {
        case QVariant::DateTime:
            v[index + i] = QVariant(qMakeDate(fld.data));
            break;
        case QVariant::Double:
        case QVariant::Int:
        case QVariant::LongLong:
            if (d->q->numericalPrecisionPolicy() != QSql::HighPrecision) {
                if ((d->q->numericalPrecisionPolicy() == QSql::LowPrecisionDouble)
                    && (fld.typ == QVariant::Double)) {
                    v[index + i] = *reinterpret_cast<double *>(fld.data);
                    break;
                }
                else if ((d->q->numericalPrecisionPolicy() == QSql::LowPrecisionInt64)
                    && (fld.typ == QVariant::LongLong)) {
                    qint64 qll = 0;
                    int r = ACINumberToInt(d->err, reinterpret_cast<ACINumber *>(fld.data), sizeof(qint64),
                        ACI_NUMBER_SIGNED, &qll);
                    if (r == ACI_SUCCESS)
                        v[index + i] = qll;
                    else
                        v[index + i] = QVariant();
                    break;
                }
                else if ((d->q->numericalPrecisionPolicy() == QSql::LowPrecisionInt32)
                    && (fld.typ == QVariant::Int)) {
                    v[index + i] = *reinterpret_cast<int *>(fld.data);
                    break;
                }
            }
            // else fall through
        case QVariant::String:
            //v[index + i] = QString(reinterpret_cast<const QChar *>(fld.data));
            // TODO zhaoj
        {
            v[index + i] = QString::fromUtf8(fld.data);
            break;
        }
            
        case QVariant::ByteArray:
            if (fld.len > 0)
                v[index + i] = QByteArray(fld.data, fld.len);
            else
                v[index + i] = QVariant(QVariant::ByteArray);
            break;
        default:
            qWarning("QACICols::value: unknown data type");
            break;
        }
    }
}

QACIResultPrivate::QACIResultPrivate(QACIResult *result, const QACIDriverPrivate *driver)
    : cols(0), q(result), env(driver->env), err(0), svc(const_cast<ACISvcCtx*&>(driver->svc)),
    sql(0), transaction(driver->transaction), serverVersion(driver->serverVersion),
    prefetchRows(driver->prefetchRows), prefetchMem(driver->prefetchMem)
{
    int r = ACIHandleAlloc(env,
        reinterpret_cast<void **>(&err),
        ACI_HTYPE_ERROR,
        0,
        0);
    if (r != 0)
        qWarning("QACIResult: unable to alloc error handle");
}

QACIResultPrivate::~QACIResultPrivate()
{
    delete cols;

    int r = ACIHandleFree(err, ACI_HTYPE_ERROR);
    if (r != 0)
        qWarning("~QACIResult: unable to free statement handle");
}


////////////////////////////////////////////////////////////////////////////

QACIResult::QACIResult(const QACIDriver * db, const QACIDriverPrivate* p)
    : QSqlCachedResult(db)
{
    d = new QACIResultPrivate(this, p);
}

QACIResult::~QACIResult()
{
    if (d->sql) {
        int r = ACIHandleFree(d->sql, ACI_HTYPE_STMT);
        if (r != 0)
            qWarning("~QACIResult: unable to free statement handle");
    }
    delete d;
}

QVariant QACIResult::handle() const
{
    return QVariant::fromValue(d->sql);
}

bool QACIResult::reset(const QString& query)
{
    if (!prepare(query))
        return false;
    return exec();
}

bool QACIResult::gotoNext(QSqlCachedResult::ValueCache &values, int index)
{
    if (at() == QSql::AfterLastRow)
        return false;

    bool piecewise = false;
    int r = ACI_SUCCESS;
    
    r = ACIStmtFetch(d->sql, d->err, 1, ACI_FETCH_NEXT, ACI_DEFAULT);

    if (index < 0) //not interested in values
        return r == ACI_SUCCESS || r == ACI_SUCCESS_WITH_INFO;

    switch (r) {
    case ACI_SUCCESS:
        break;
    case ACI_SUCCESS_WITH_INFO:
        qOraWarning("QACIResult::gotoNext: SuccessWithInfo: ", d->err);
        r = ACI_SUCCESS; //ignore it
        break;
    case ACI_NO_DATA:
        // end of rowset
        return false;
    case ACI_NEED_DATA:
        piecewise = true;
        r = ACI_SUCCESS;
        break;
    case ACI_ERROR:
        if (qOraErrorNumber(d->err) == 1406) {
            qWarning() << "QACI Warning: data truncated for:" << lastQuery();
            r = ACI_SUCCESS; /* ignore it */
            break;
        }
        // fall through
    default:
        qOraWarning("QACIResult::gotoNext: ", d->err);
        setLastError(qMakeError(QCoreApplication::translate("QACIResult",
            "Unable to goto next"),
            QSqlError::StatementError, d->err));
        break;
    }

    // need to read piecewise before assigning values
    if (r == ACI_SUCCESS && piecewise)
        r = d->cols->readPiecewise(values, index);

    if (r == ACI_SUCCESS)
        d->cols->getValues(values, index);
    if (r == ACI_SUCCESS)
        r = d->cols->readLOBs(values, index);
    if (r != ACI_SUCCESS)
        setAt(QSql::AfterLastRow);
    return r == ACI_SUCCESS || r == ACI_SUCCESS_WITH_INFO;
}

int QACIResult::size()
{
    return -1;
}

int QACIResult::numRowsAffected()
{
    int rowCount;
    ACIAttrGet(d->sql,
        ACI_HTYPE_STMT,
        &rowCount,
        NULL,
        ACI_ATTR_ROW_COUNT,
        d->err);
    return rowCount;
}

bool QACIResult::prepare(const QString& query)
{
    int r = 0;
    QSqlResult::prepare(query);

    delete d->cols;
    d->cols = 0;
    QSqlCachedResult::cleanup();

    if (d->sql) {
        r = ACIHandleFree(d->sql, ACI_HTYPE_STMT);
        if (r != ACI_SUCCESS)
            qOraWarning("QACIResult::prepare: unable to free statement handle:", d->err);
    }
    if (query.isEmpty())
        return false;
    r = ACIHandleAlloc(d->env,
        reinterpret_cast<void **>(&d->sql),
        ACI_HTYPE_STMT,
        0,
        0);
    if (r != ACI_SUCCESS) {
        qOraWarning("QACIResult::prepare: unable to alloc statement:", d->err);
        setLastError(qMakeError(QCoreApplication::translate("QACIResult",
            "Unable to alloc statement"), QSqlError::StatementError, d->err));
        return false;
    }
    d->setStatementAttributes();
    QByteArray queryByte = query.toUtf8();
    const OraText *txt = reinterpret_cast<const OraText *>(queryByte.data());
    const int len = queryByte.length();
    r = ACIStmtPrepare(d->sql,
        d->err,
        txt,
        (ub4)len,
        ACI_NTV_SYNTAX,
        ACI_DEFAULT);
    if (r != ACI_SUCCESS) {
        qOraWarning("QACIResult::prepare: unable to prepare statement:", d->err);
        setLastError(qMakeError(QCoreApplication::translate("QACIResult",
            "Unable to prepare statement"), QSqlError::StatementError, d->err));
        return false;
    }
    return true;
}

bool QACIResult::exec()
{
    int r = 0;
    ub2 stmtType = 0;
    ub4 iters;
    ub4 mode;
    QList<QByteArray> tmpStorage;
    IndicatorArray indicators(boundValueCount());
    SizeArray tmpSizes(boundValueCount());

    r = ACIAttrGet(d->sql,
        ACI_HTYPE_STMT,
        &stmtType,
        NULL,
        ACI_ATTR_STMT_TYPE,
        d->err);

    if (r != ACI_SUCCESS && r != ACI_SUCCESS_WITH_INFO) {
        qOraWarning("QACIResult::exec: Unable to get statement type:", d->err);
        setLastError(qMakeError(QCoreApplication::translate("QACIResult",
            "Unable to get statement type"), QSqlError::StatementError, d->err));
#ifdef QACI_DEBUG
        qDebug() << "lastQuery()" << lastQuery();
#endif
        return false;
    }

    iters = stmtType == ACI_STMT_SELECT ? 0 : 1;
    mode = d->transaction ? ACI_DEFAULT : ACI_COMMIT_ON_SUCCESS;

    // bind placeholders
    if (boundValueCount() > 0
        && d->bindValues(boundValues(), indicators, tmpSizes, tmpStorage) != ACI_SUCCESS) {
        qOraWarning("QACIResult::exec: unable to bind value: ", d->err);
        setLastError(qMakeError(QCoreApplication::translate("QACIResult", "Unable to bind value"),
            QSqlError::StatementError, d->err));
#ifdef QACI_DEBUG
        qDebug() << "lastQuery()" << lastQuery();
#endif
        return false;
    }

    // execute
    r = ACIStmtExecute(d->svc,
        d->sql,
        d->err,
        iters,
        0,
        0,
        0,
        mode);
    if (r != ACI_SUCCESS && r != ACI_SUCCESS_WITH_INFO) {
        qOraWarning("QACIResult::exec: unable to execute statement:", d->err);
        setLastError(qMakeError(QCoreApplication::translate("QACIResult",
            "Unable to execute statement"), QSqlError::StatementError, d->err));
#ifdef QACI_DEBUG
        qDebug() << "lastQuery()" << lastQuery();
#endif
        return false;
    }

    if (stmtType == ACI_STMT_SELECT) {
        ub4 parmCount = 0;
        int r = ACIAttrGet(d->sql, ACI_HTYPE_STMT, reinterpret_cast<void **>(&parmCount),
            0, ACI_ATTR_PARAM_COUNT, d->err);
        if (r == 0 && !d->cols)
            d->cols = new QACICols(parmCount, d);
        setSelect(true);
        QSqlCachedResult::init(parmCount);
    }
    else { /* non-SELECT */
        setSelect(false);
    }
    setAt(QSql::BeforeFirstRow);
    setActive(true);

    if (hasOutValues())
        d->outValues(boundValues(), indicators, tmpStorage);

    return true;
}

QSqlRecord QACIResult::record() const
{
    QSqlRecord inf;
    if (!isActive() || !isSelect() || !d->cols)
        return inf;
    return d->cols->rec;
}

QVariant QACIResult::lastInsertId() const
{
    if (isActive()) {
        QACIRowIdPointer ptr(new QACIRowId(d->env));

        int r = ACIAttrGet(d->sql, ACI_HTYPE_STMT, ptr.constData()->id,
            0, ACI_ATTR_ROWID, d->err);
        if (r == ACI_SUCCESS)
            return QVariant::fromValue(ptr);
    }
    return QVariant();
}

bool QACIResult::execBatch(bool arrayBind)
{
    QACICols::execBatch(d, boundValues(), arrayBind);
    resetBindCount();
    return lastError().type() == QSqlError::NoError;
}

void QACIResult::virtual_hook(int id, void *data)
{
    Q_ASSERT(data);

    QSqlCachedResult::virtual_hook(id, data);
}

////////////////////////////////////////////////////////////////////////////


QACIDriver::QACIDriver(QObject* parent)
    : QSqlDriver(parent)
{
    Q_D(QACIDriver);
#ifdef QACI_THREADED
    const ub4 mode = ACI_OBJECT | ACI_THREADED;
#else
    const ub4 mode = ACI_OBJECT;
#endif
    int r = ACIEnvNlsCreate(&d->env,
        mode,
        NULL,
        NULL,
        NULL,
        NULL,
        0,
        NULL, ACE_UTF8ID, ACE_UTF8ID);//UTF8 Used
    if (r != 0) {
        qWarning("QACIDriver: unable to create environment");
        setLastError(qMakeError(tr("Unable to initialize", "QACIDriver"),
            QSqlError::ConnectionError, d->err));
        return;
    }

    d->allocErrorHandle();
}

QACIDriver::QACIDriver(ACIEnv* env, ACISvcCtx* ctx, QObject* parent)
    : QSqlDriver(parent)
{
    Q_D(QACIDriver);
    d->env = env;
    d->svc = ctx;

    d->allocErrorHandle();

    if (env && ctx) {
        setOpen(true);
        setOpenError(false);
    }
}

QACIDriver::~QACIDriver()
{
    Q_D(QACIDriver);
    if (isOpen())
        close();
    int r = ACIHandleFree(d->err, ACI_HTYPE_ERROR);
    if (r != ACI_SUCCESS)
        qWarning("Unable to free Error handle: %d", r);
    r = ACIHandleFree(d->env, ACI_HTYPE_ENV);
    if (r != ACI_SUCCESS)
        qWarning("Unable to free Environment handle: %d", r);
}

bool QACIDriver::hasFeature(DriverFeature f) const
{
    Q_D(const QACIDriver);
    switch (f) {
    case Transactions:
    case LastInsertId:
    case BLOB:
    case PreparedQueries:
    case NamedPlaceholders:
    case BatchOperations:
    case LowPrecisionNumbers:
        return true;
    case QuerySize:
    case PositionalPlaceholders:
    case SimpleLocking:
    case EventNotifications:
    case FinishQuery:
    case CancelQuery:
    case MultipleResultSets:
        return false;
    case Unicode:
        return d->serverVersion >= 9;
    }
    return false;
}

static void qParseOpts(const QString &options, QACIDriverPrivate *d)
{
    const QStringList opts(options.split(QLatin1Char(';'), QString::SkipEmptyParts));
    for (int i = 0; i < opts.count(); ++i) {
        const QString tmp(opts.at(i));
        int idx;
        if ((idx = tmp.indexOf(QLatin1Char('='))) == -1) {
            qWarning("QACIDriver::parseArgs: Invalid parameter: '%s'",
                tmp.toLocal8Bit().constData());
            continue;
        }
        const QString opt = tmp.left(idx);
        const QString val = tmp.mid(idx + 1).simplified();
        bool ok;
        if (opt == QLatin1String("ACI_ATTR_PREFETCH_ROWS")) {
            d->prefetchRows = val.toInt(&ok);
            if (!ok)
                d->prefetchRows = -1;
        }
        else if (opt == QLatin1String("ACI_ATTR_PREFETCH_MEMORY")) {
            d->prefetchMem = val.toInt(&ok);
            if (!ok)
                d->prefetchMem = -1;
        }
        else {
            qWarning("QACIDriver::parseArgs: Invalid parameter: '%s'",
                opt.toLocal8Bit().constData());
        }
    }
}

bool QACIDriver::open(const QString & db,
    const QString & user,
    const QString & password,
    const QString & hostname,
    int port,
    const QString &opts)
{
    Q_D(QACIDriver);
    int r;

    if (isOpen())
        close();

#ifdef QACI_DEBUG
    // get charset
    ub4 charset = 0;
    r = ACIAttrGet(d->env, ACI_HTYPE_ENV,
        &charset, 0, ACI_ATTR_ENV_CHARSET_ID, d->err);

    qDebug() << "ACI Client Driver use charset :" << charset;

    Q_ASSERT(charset == ACE_UTF8ID);
#endif

    qParseOpts(opts, d);

    QString connectionString = db;
    if (!hostname.isEmpty())
        connectionString =
        QStringLiteral("%1:%2/%3").arg(hostname).arg((port > -1 ? port : 1521)).arg(db);

    r = ACIHandleAlloc(d->env, reinterpret_cast<void **>(&d->srvhp), ACI_HTYPE_SERVER, 0, 0);
    if (r == ACI_SUCCESS){
        QByteArray bytearray = connectionString.toUtf8();
        const OraText * aciconn = reinterpret_cast<const OraText *>(bytearray.data());
        r = ACIServerAttach(d->srvhp, d->err, aciconn,
            bytearray.length(), ACI_DEFAULT);
    }
    if (r == ACI_SUCCESS || r == ACI_SUCCESS_WITH_INFO)
        r = ACIHandleAlloc(d->env, reinterpret_cast<void **>(&d->svc), ACI_HTYPE_SVCCTX, 0, 0);
    if (r == ACI_SUCCESS)
        r = ACIAttrSet(d->svc, ACI_HTYPE_SVCCTX, d->srvhp, 0, ACI_ATTR_SERVER, d->err);
    if (r == ACI_SUCCESS)
        r = ACIHandleAlloc(d->env, reinterpret_cast<void **>(&d->authp), ACI_HTYPE_SESSION, 0, 0);
    if (r == ACI_SUCCESS) {
        QByteArray userByte = user.toUtf8();
        r = ACIAttrSet(d->authp, ACI_HTYPE_SESSION, userByte.data(),
            userByte.length(), ACI_ATTR_USERNAME, d->err);
    }
    if (r == ACI_SUCCESS) {
        QByteArray passByte = password.toUtf8();
        r = ACIAttrSet(d->authp, ACI_HTYPE_SESSION, passByte.data(),
            passByte.length(), ACI_ATTR_PASSWORD, d->err);
    }
        

    ACITrans* trans;
    if (r == ACI_SUCCESS)
        r = ACIHandleAlloc(d->env, reinterpret_cast<void **>(&trans), ACI_HTYPE_TRANS, 0, 0);
    if (r == ACI_SUCCESS)
        r = ACIAttrSet(d->svc, ACI_HTYPE_SVCCTX, trans, 0, ACI_ATTR_TRANS, d->err);

    if (r == ACI_SUCCESS) {
        if (user.isEmpty() && password.isEmpty())
            r = ACISessionBegin(d->svc, d->err, d->authp, ACI_CRED_EXT, ACI_DEFAULT);
        else
            r = ACISessionBegin(d->svc, d->err, d->authp, ACI_CRED_RDBMS, ACI_DEFAULT);
    }
    if (r == ACI_SUCCESS || r == ACI_SUCCESS_WITH_INFO)
        r = ACIAttrSet(d->svc, ACI_HTYPE_SVCCTX, d->authp, 0, ACI_ATTR_SESSION, d->err);

    if (r != ACI_SUCCESS) {
        setLastError(qMakeError(tr("Unable to logon"), QSqlError::ConnectionError, d->err));
        setOpenError(true);
        if (d->authp)
            ACIHandleFree(d->authp, ACI_HTYPE_SESSION);
        d->authp = 0;
        if (d->srvhp)
            ACIHandleFree(d->srvhp, ACI_HTYPE_SERVER);
        d->srvhp = 0;
        return false;
    }

    // get server version
    char vertxt[512];
    r = ACIServerVersion(d->svc,
        d->err,
        reinterpret_cast<OraText *>(vertxt),
        sizeof(vertxt),
        ACI_HTYPE_SVCCTX);
    if (r != 0) {
        qWarning("QACIDriver::open: could not get Oracle server version.");
    }
    else {
        QString versionStr = QString::fromUtf8(vertxt);
#ifdef QACI_DEBUG
        qDebug() << "Get ACI Server Version:" << versionStr;
#endif
        QRegExp vers(QLatin1String("([0-9]+)\\.[0-9\\.]+[0-9]"));
        if (vers.indexIn(versionStr) >= 0)
            d->serverVersion = vers.cap(1).toInt();
        if (d->serverVersion == 0)
            d->serverVersion = -1;
    }

    setOpen(true);
    setOpenError(false);
    d->user= user;

    return true;
}

void QACIDriver::close()
{
    Q_D(QACIDriver);
    if (!isOpen())
        return;

    ACISessionEnd(d->svc, d->err, d->authp, ACI_DEFAULT);
    ACIServerDetach(d->srvhp, d->err, ACI_DEFAULT);
    ACIHandleFree(d->authp, ACI_HTYPE_SESSION);
    d->authp = 0;
    ACIHandleFree(d->srvhp, ACI_HTYPE_SERVER);
    d->srvhp = 0;
    ACIHandleFree(d->svc, ACI_HTYPE_SVCCTX);
    d->svc = 0;
    setOpen(false);
    setOpenError(false);
}

QSqlResult *QACIDriver::createResult() const
{
    Q_D(const QACIDriver);
    return new QACIResult(this, d);
}

bool QACIDriver::beginTransaction()
{
    Q_D(QACIDriver);
    if (!isOpen()) {
        qWarning("QACIDriver::beginTransaction: Database not open");
        return false;
    }
    int r = ACITransStart(d->svc,
        d->err,
        2,
        ACI_TRANS_READWRITE);
    if (r == ACI_ERROR) {
        qOraWarning("QACIDriver::beginTransaction: ", d->err);
        setLastError(qMakeError(QCoreApplication::translate("QACIDriver",
            "Unable to begin transaction"), QSqlError::TransactionError, d->err));
        return false;
    }
    d->transaction = true;
    return true;
}

bool QACIDriver::commitTransaction()
{
    Q_D(QACIDriver);
    if (!isOpen()) {
        qWarning("QACIDriver::commitTransaction: Database not open");
        return false;
    }
    int r = ACITransCommit(d->svc,
        d->err,
        0);
    if (r == ACI_ERROR) {
        qOraWarning("QACIDriver::commitTransaction:", d->err);
        setLastError(qMakeError(QCoreApplication::translate("QACIDriver",
            "Unable to commit transaction"), QSqlError::TransactionError, d->err));
        return false;
    }
    d->transaction = false;
    return true;
}

bool QACIDriver::rollbackTransaction()
{
    Q_D(QACIDriver);
    if (!isOpen()) {
        qWarning("QACIDriver::rollbackTransaction: Database not open");
        return false;
    }
    int r = ACITransRollback(d->svc,
        d->err,
        0);
    if (r == ACI_ERROR) {
        qOraWarning("QACIDriver::rollbackTransaction:", d->err);
        setLastError(qMakeError(QCoreApplication::translate("QACIDriver",
            "Unable to rollback transaction"), QSqlError::TransactionError, d->err));
        return false;
    }
    d->transaction = false;
    return true;
}

QStringList QACIDriver::tables(QSql::TableType type) const
{
    Q_D(const QACIDriver);
    QStringList tl;
    QStringList sysUsers = QStringList() << QLatin1String("MDSYS")
        << QLatin1String("LBACSYS")
        << QLatin1String("SYS")
        << QLatin1String("SYSTEM")
        << QLatin1String("WKSYS")
        << QLatin1String("CTXSYS")
        << QLatin1String("WMSYS");

    QString user = d->user;
    if (isIdentifierEscaped(user, QSqlDriver::TableName))
        user = stripDelimiters(user, QSqlDriver::TableName);
    else
        user = user.toUpper();

    if (sysUsers.contains(user))
        sysUsers.removeAll(user);;

    if (!isOpen())
        return tl;

    QSqlQuery t(createResult());
    t.setForwardOnly(true);
    if (type & QSql::Tables) {
        QString query = QLatin1String("select owner, table_name from all_tables where ");
        QStringList whereList;
        foreach(const QString &sysUserName, sysUsers)
            whereList << QLatin1String("owner != '") + sysUserName + QLatin1String("' ");
        t.exec(query + whereList.join(QLatin1String(" and ")));

        while (t.next()) {
            if (t.value(0).toString().toUpper() != user.toUpper())
                tl.append(t.value(0).toString() + QLatin1Char('.') + t.value(1).toString());
            else
                tl.append(t.value(1).toString());
        }

        // list all table synonyms as well
        query = QLatin1String("select owner, synonym_name from all_synonyms where ");
        t.exec(query + whereList.join(QLatin1String(" and ")));
        while (t.next()) {
            if (t.value(0).toString() != d->user)
                tl.append(t.value(0).toString() + QLatin1Char('.') + t.value(1).toString());
            else
                tl.append(t.value(1).toString());
        }
    }
    if (type & QSql::Views) {
        QString query = QLatin1String("select owner, view_name from all_views where ");
        QStringList whereList;
        foreach(const QString &sysUserName, sysUsers)
            whereList << QLatin1String("owner != '") + sysUserName + QLatin1String("' ");
        t.exec(query + whereList.join(QLatin1String(" and ")));
        while (t.next()) {
            if (t.value(0).toString().toUpper() != d->user.toUpper())
                tl.append(t.value(0).toString() + QLatin1Char('.') + t.value(1).toString());
            else
                tl.append(t.value(1).toString());
        }
    }
    if (type & QSql::SystemTables) {
        t.exec(QLatin1String("select table_name from dictionary"));
        while (t.next()) {
            tl.append(t.value(0).toString());
        }
        QString query = QLatin1String("select owner, table_name from all_tables where ");
        QStringList whereList;
        foreach(const QString &sysUserName, sysUsers)
            whereList << QLatin1String("owner = '") + sysUserName + QLatin1String("' ");
        t.exec(query + whereList.join(QLatin1String(" or ")));

        while (t.next()) {
            if (t.value(0).toString().toUpper() != user.toUpper())
                tl.append(t.value(0).toString() + QLatin1Char('.') + t.value(1).toString());
            else
                tl.append(t.value(1).toString());
        }

        // list all table synonyms as well
        query = QLatin1String("select owner, synonym_name from all_synonyms where ");
        t.exec(query + whereList.join(QLatin1String(" or ")));
        while (t.next()) {
            if (t.value(0).toString() != d->user)
                tl.append(t.value(0).toString() + QLatin1Char('.') + t.value(1).toString());
            else
                tl.append(t.value(1).toString());
        }
    }
    return tl;
}

void qSplitTableAndOwner(const QString & tname, QString * tbl,
    QString * owner)
{
    int i = tname.indexOf(QLatin1Char('.')); // prefixed with owner?
    if (i != -1) {
        *tbl = tname.right(tname.length() - i - 1);
        *owner = tname.left(i);
    }
    else {
        *tbl = tname;
    }
}

QSqlRecord QACIDriver::record(const QString& tablename) const
{
    Q_D(const QACIDriver);
    QSqlRecord fil;
    if (!isOpen())
        return fil;

    QSqlQuery t(createResult());
    // using two separate queries for this is A LOT faster than using
    // eg. a sub-query on the sys.synonyms table
    QString stmt(QLatin1String("select column_name, data_type, data_length, "
        "data_precision, data_scale, nullable, data_default%1"
        "from all_tab_columns a "
        "where a.table_name=%2"));
    if (d->serverVersion >= 9)
        stmt = stmt.arg(QLatin1String(", char_length "));
    else
        stmt = stmt.arg(QLatin1String(" "));
    bool buildRecordInfo = false;
    QString table, owner, tmpStmt;
    qSplitTableAndOwner(tablename, &table, &owner);

    if (isIdentifierEscaped(table, QSqlDriver::TableName))
        table = stripDelimiters(table, QSqlDriver::TableName);
    else
        table = table.toUpper();

    tmpStmt = stmt.arg(QLatin1Char('\'') + table + QLatin1Char('\''));
    if (owner.isEmpty()) {
        owner = d->user;
    }

    if (isIdentifierEscaped(owner, QSqlDriver::TableName))
        owner = stripDelimiters(owner, QSqlDriver::TableName);
    else
        owner = owner.toUpper();

    tmpStmt += QLatin1String(" and a.owner='") + owner + QLatin1Char('\'');
    t.setForwardOnly(true);
    t.exec(tmpStmt);
    if (!t.next()) { // try and see if the tablename is a synonym
        stmt = stmt + QLatin1String(" join all_synonyms b "
            "on a.owner=b.table_owner and a.table_name=b.table_name "
            "where b.owner='") + owner +
            QLatin1String("' and b.synonym_name='") + table +
            QLatin1Char('\'');
        t.setForwardOnly(true);
        t.exec(stmt);
        if (t.next())
            buildRecordInfo = true;
    }
    else {
        buildRecordInfo = true;
    }
    QStringList keywords = QStringList() << QLatin1String("NUMBER") << QLatin1String("FLOAT") << QLatin1String("BINARY_FLOAT")
        << QLatin1String("BINARY_DOUBLE");
    if (buildRecordInfo) {
        do {
            QVariant::Type ty = qDecodeACIType(t.value(1).toString(), t.numericalPrecisionPolicy());
            QSqlField f(t.value(0).toString(), ty);
            f.setRequired(t.value(5).toString() == QLatin1String("N"));
            f.setPrecision(t.value(4).toInt());
            if (d->serverVersion >= 9 && (ty == QVariant::String) && !t.isNull(3) && !keywords.contains(t.value(1).toString())) {
                // Oracle9: data_length == size in bytes, char_length == amount of characters
                f.setLength(t.value(7).toInt());
            }
            else {
                f.setLength(t.value(t.isNull(3) ? 2 : 3).toInt());
            }
            f.setDefaultValue(t.value(6));
            fil.append(f);
        } while (t.next());
    }
    return fil;
}

QSqlIndex QACIDriver::primaryIndex(const QString& tablename) const
{
    Q_D(const QACIDriver);
    QSqlIndex idx(tablename);
    if (!isOpen())
        return idx;
    QSqlQuery t(createResult());
    QString stmt(QLatin1String("select b.column_name, b.index_name, a.table_name, a.owner "
        "from all_constraints a, all_ind_columns b "
        "where a.constraint_type='P' "
        "and b.index_name = a.constraint_name "
        "and b.index_owner = a.owner"));

    bool buildIndex = false;
    QString table, owner, tmpStmt;
    qSplitTableAndOwner(tablename, &table, &owner);

    if (isIdentifierEscaped(table, QSqlDriver::TableName))
        table = stripDelimiters(table, QSqlDriver::TableName);
    else
        table = table.toUpper();

    tmpStmt = stmt + QLatin1String(" and a.table_name='") + table + QLatin1Char('\'');
    if (owner.isEmpty()) {
        owner = d->user;
    }

    if (isIdentifierEscaped(owner, QSqlDriver::TableName))
        owner = stripDelimiters(owner, QSqlDriver::TableName);
    else
        owner = owner.toUpper();

    tmpStmt += QLatin1String(" and a.owner='") + owner + QLatin1Char('\'');
    t.setForwardOnly(true);
    t.exec(tmpStmt);

    if (!t.next()) {
        stmt += QLatin1String(" and a.table_name=(select tname from sys.synonyms "
            "where sname='") + table + QLatin1String("' and creator=a.owner)");
        t.setForwardOnly(true);
        t.exec(stmt);
        if (t.next()) {
            owner = t.value(3).toString();
            buildIndex = true;
        }
    }
    else {
        buildIndex = true;
    }
    if (buildIndex) {
        QSqlQuery tt(createResult());
        tt.setForwardOnly(true);
        idx.setName(t.value(1).toString());
        do {
            tt.exec(QLatin1String("select data_type from all_tab_columns where table_name='") +
                t.value(2).toString() + QLatin1String("' and column_name='") +
                t.value(0).toString() + QLatin1String("' and owner='") +
                owner + QLatin1Char('\''));
            if (!tt.next()) {
                return QSqlIndex();
            }
            QSqlField f(t.value(0).toString(), qDecodeACIType(tt.value(0).toString(), t.numericalPrecisionPolicy()));
            idx.append(f);
        } while (t.next());
        return idx;
    }
    return QSqlIndex();
}

QString QACIDriver::formatValue(const QSqlField &field, bool trimStrings) const
{
    switch (field.type()) {
    case QVariant::DateTime: {
        QDateTime datetime = field.value().toDateTime();
        QString datestring;
        if (datetime.isValid()) {
            datestring = QLatin1String("TO_DATE('") + QString::number(datetime.date().year())
                + QLatin1Char('-')
                + QString::number(datetime.date().month()) + QLatin1Char('-')
                + QString::number(datetime.date().day()) + QLatin1Char(' ')
                + QString::number(datetime.time().hour()) + QLatin1Char(':')
                + QString::number(datetime.time().minute()) + QLatin1Char(':')
                + QString::number(datetime.time().second())
                + QLatin1String("','YYYY-MM-DD HH24:MI:SS')");
        }
        else {
            datestring = QLatin1String("NULL");
        }
        return datestring;
    }
    case QVariant::Time: {
        QDateTime datetime = field.value().toDateTime();
        QString datestring;
        if (datetime.isValid()) {
            datestring = QLatin1String("TO_DATE('")
                + QString::number(datetime.time().hour()) + QLatin1Char(':')
                + QString::number(datetime.time().minute()) + QLatin1Char(':')
                + QString::number(datetime.time().second())
                + QLatin1String("','HH24:MI:SS')");
        }
        else {
            datestring = QLatin1String("NULL");
        }
        return datestring;
    }
    case QVariant::Date: {
        QDate date = field.value().toDate();
        QString datestring;
        if (date.isValid()) {
            datestring = QLatin1String("TO_DATE('") + QString::number(date.year()) +
                QLatin1Char('-') +
                QString::number(date.month()) + QLatin1Char('-') +
                QString::number(date.day()) + QLatin1String("','YYYY-MM-DD')");
        }
        else {
            datestring = QLatin1String("NULL");
        }
        return datestring;
    }
    default:
        break;
    }
    return QSqlDriver::formatValue(field, trimStrings);
}

QVariant QACIDriver::handle() const
{
    Q_D(const QACIDriver);
    return QVariant::fromValue(d->env);
}

QString QACIDriver::escapeIdentifier(const QString &identifier, IdentifierType type) const
{
    QString res = identifier;
    if (!identifier.isEmpty() && !isIdentifierEscaped(identifier, type)) {
        res.replace(QLatin1Char('"'), QLatin1String("\"\""));
        res.prepend(QLatin1Char('"')).append(QLatin1Char('"'));
        res.replace(QLatin1Char('.'), QLatin1String("\".\""));
    }
    return res;
}

QT_END_NAMESPACE
