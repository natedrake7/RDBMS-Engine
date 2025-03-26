#include <vector>
#include "../Field/Field.h"
using namespace std;


class JoinField{
    Field firstTableCondition;
    Field secondTableCondition;
    vector<JoinField> children;

    public:
        explicit JoinField(const Field& firstTableField, const Field& secondTableField);
        const Field& GetFirstTableCondition() const;
        const Field& GetSecondTableCondition() const;
};