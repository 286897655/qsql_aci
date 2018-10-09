
#ifndef QSQL_ACI_H
#define QSQL_ACI_H

#include <QtSql/qsqlresult.h>
#include <QtSql/qsqldriver.h>

typedef struct ACIEnv ACIEnv;
typedef struct ACISvcCtx ACISvcCtx;

QT_BEGIN_NAMESPACE

class QACIDriver;
class QACICols;
class QACIDriverPrivate;

class QACIDriver : public QSqlDriver
{
    Q_DECLARE_PRIVATE(QACIDriver)
        Q_OBJECT
        friend struct QACIResultPrivate;
    friend class QACIPrivate;
public:
    explicit QACIDriver(QObject* parent = 0);
    QACIDriver(ACIEnv* env, ACISvcCtx* ctx, QObject* parent = 0);
    ~QACIDriver();
    bool hasFeature(DriverFeature f) const;
    bool open(const QString & db,
        const QString & user,
        const QString & password,
        const QString & host,
        int port,
        const QString& connOpts);
    void close();
    QSqlResult *createResult() const;
    QStringList tables(QSql::TableType) const;
    QSqlRecord record(const QString& tablename) const;
    QSqlIndex primaryIndex(const QString& tablename) const;
    QString formatValue(const QSqlField &field,
        bool trimStrings) const;
    QVariant handle() const;
    QString escapeIdentifier(const QString &identifier, IdentifierType) const;

protected:
    bool                beginTransaction();
    bool                commitTransaction();
    bool                rollbackTransaction();
};

QT_END_NAMESPACE

#endif // QSQL_ACI_H

