#ifndef METADATA_H
#define METADATA_H

/*! \file */
#include <list>
#include <vector>
#include <map>
#include <memory>
#include <mutex>
#include "rtree.h"
#include "types.h"
#include "savime.h"
#include "dynamic_bitset.h"
#define UNSAVED_ID -1
#define INDEX_SEPARATOR ':'

/*TARS' hard limits*/
#define MAX_NUM_TARS_IN_TARS  std::numeric_limits<int32_t>::max()
#define MAX_NUM_DATASETS_IN_TARS  std::numeric_limits<int64_t>::max()
#define MAX_NUM_OF_TYPES_IN_TARS  std::numeric_limits<int32_t>::max()
#define MAX_NUM_OF_ROLES_IN_TYPE  std::numeric_limits<uint8_t>::max()

/*TAR's hard limits*/
#define MAX_NUM_OF_DATA_ELEMENTS  std::numeric_limits<uint8_t>::max()
#define MAX_NUM_OF_DIMS_IN_TAR  std::numeric_limits<uint8_t>::max()
#define MAX_NUM_OF_ATT_IN_TAR   std::numeric_limits<uint8_t>::max()
#define MAX_NUM_SUBTARS_IN_TAR std::numeric_limits<int64_t>::max()
#define MAX_TAR_LEN std::numeric_limits<uint64_t>::max()

/*Other hard limits*/
#define MAX_VECTOR_ATT_LEN  std::numeric_limits<uint32_t>::max()
#define MAX_DIM_BOUND (RealIndex)(1.0/std::numeric_limits<double>::epsilon())
#define MAX_DIM_LEN (RealIndex)(1.0/std::numeric_limits<double>::epsilon())
#define MIN_SPACING std::numeric_limits<float>::min()
#define MAX_SUBTAR_LEN std::numeric_limits<int64_t>::max()
#define MAX_ENTRIES_IN_DATASET std::numeric_limits<int64_t>::max()


  
using namespace std;

class TAR;
class Subtar;
class MetadataObject;
typedef MetadataObject* MetadataObjectPtr;
class MetadataObjectListener;
#define INFO_TYPE_DISTINCT_COUNT "distinct_count"

typedef std::shared_ptr<MetadataObjectListener> MetadataObjectListenerPtr;
typedef std::shared_ptr<Subtar> SubtarPtr;
typedef std::shared_ptr<RTree<int64_t, int64_t>> SubtarsIndex;
typedef std::shared_ptr<TAR> TARPtr;

extern const char * dataTypeNames[];
extern const char * dimTypeNames[];
extern const char * specsTypeNames[];

/**
 * Enum with codes for possible types of a DataElement. A data element is a
 * component
 * of a TAR (Typed Array). It can be either a dimension, defining the structure
 * of
 * the TAR, or an attribute, defining an element in a TAR cell.
 */
typedef enum {
  DIMENSION_SCHEMA_ELEMENT, /*!<Indicates a dimension data element. */
  ATTRIBUTE_SCHEMA_ELEMENT  /*!<Indicates an attribute data element. */

} DataElementType;

/**
* Enum with codes for the dimension types.
*/
typedef enum {
  EXPLICIT, /*!<Explicit dimensions have their index values explicitly store in
             * a mapping array since they cannot be inferred.
             * It is the case for sparse or irregular dimensions, whose index
             * values do not conform a well behaved pattern. */
  IMPLICIT, /*!<Implicit dimensions do not have their index values explicitly
             * store since they can be inferred.
             * It is the case for dense and regular dimensions, whose index
             * values conform a well behaved ordened pattern.*/
  NO_DIM_TYPE /*!<Code for invalid dimension types. */

} DimensionType;

/**
* Enum with codes for possible types of a DimensionSpecification instance.
*/
typedef enum {
  ORDERED, /*!<A ordered dimension specification is an implicit range of ordered
            * and equally spaced indexes implicilty defined
             * by an upper and lower bound. It is the simplest possible case for
            * a dimension specification.*/
  PARTIAL, /*!<A partial dimension specification is a explicit list of values,
            * one for each different index value. It is the case for
            * TARs with sparse but regular dimensions, meaning that dimensions
            * values remain consistent with regard to all other
            * dimensions in the subtar.*/
  TOTAL,   /*!<A total dimension specification is a explicit list of values, one
            * for each possible position in a TAR. It is the case for
            * TARs sparse and irregular dimension. Since the TAR is totally sparse
            * and irregular, and indexes values do not remain
            * consistent with regard to other dimensions. indexes cannot be
            * inferred for any dimension.
            * If ond dimensions specificaiton for a subtar is total, then all must
            * be too.*/
  NO_SPEC_TYPE /*!<Code for invalid dimension specification type.*/
} SpecsType;

/**
* A MetadataObject is any object that stores data regarding the schema and
* structures of a SAVIME database.
*/
class MetadataObject {

protected:
  std::list<MetadataObjectListenerPtr>
      _listeners;              /*!<List of MetadaObjectListeners.*/
  std::recursive_mutex _mutex; /*!<Mutex for controlling access to the object.*/

public:
  /**
  * Adds a new MetadaObjectListener to the list.
  * @param listener is a MetadaObjectListener to be notified for events related
  * to the instance.
  */
  virtual void Addlistener(MetadataObjectListenerPtr listener) {
    _listeners.push_back(listener);
  }

  virtual void ClearListeners() {
    return _listeners.clear();
  }

  /**
  * Gets the list of MetadaObjectListener.
  * @return A list of MetadataObjectListenerPtr listening to events related to
  * the instance.
  */
  virtual list<MetadataObjectListenerPtr> &GetListerners() {
    return _listeners;
  }
};

/**
* A MetadataObjectListener is an interface implemented by modules that need to
* notified when a
* metadata object, such as a TAR or Dataset goes out of scope an also might need
* to be removed from
* the disk or database.
*/
class MetadataObjectListener {
public:
  /**
  * Notifies the MetadataObjectListener that an object must be disposed.
  * @param object is a MetadataObject to be properly disposed.
  */
  virtual void DisposeObject(MetadataObjectPtr object) = 0;

  /**
  * Notifies the MetadataObjectListener that an object must be disposed.
  * @param object is a MetadataObject to be properly disposed.
  */
  virtual string GetObjectInfo(MetadataObjectPtr object, string infoType) = 0;
};

// Metadata Objects definition
//------------------------------------------------------------------------------

/**
* A dataset is a basic structure that encapsulates an array of objects/values in
* a file or in the shared memory.
* Datasets can be attached to TARs in various contexts. They can be attached to
* attributes, forming the
* TARs cell tuples. They can be used to specify explicit dimension indexes
* values or to represent Partial and
* Total dimension specifications. It can also hold a bitmask of values as a
* result of a filtering/predicate operation.
*/
struct Dataset : public MetadataObject {
private:
  int64_t _dsId;         /*!<Identifier of the dataset in the metadata manager.*/
  string _name;          /*!<User given dataset name.*/
  string _location;      /*!<Path for the file containing the dataset data.*/
  savime_size_t _length; /*!<Length in bytes of the dataset file.*/
  savime_size_t _entry_count; /*!<Number of entries the dataset stores. Equals
                                length/sizeof(type).*/
  bool _has_indexes;  /*!<Specifies for a dataset storing a filtering result, if
                        the indexes obtained based on the bitmask are
                        available.*/
  bool _sorted;       /*!<Specifies if data values in the Dataset are sorted in
                        ascending order.*/
  DataType _type;     /*!<Type of data stored in the dataset file.*/
  BitsetPtr _bitMask; /*!<Bitmask representing the result of a predicate or
                      filtering operation. If its no-null,
                      * it means that the dataset do not stores data, but is
                      used to specify which cells of a subtar must be
                       kept after a filtering operation. The bitmask has a bit
                      for every possible position in a subtar, and its state
                       1 or 0, tells if the value must be kept or removed from
                      the dataset respectively.*/

  void Define(int64_t id, string name, string file, DataType type);

public:
  Dataset(int64_t id, string name, string file, DataType type);
  Dataset(BitsetPtr bitMask);
  Dataset(savime_size_t bitmaskSize);
  int64_t& GetId() { return _dsId; }
  string& GetName() { return _name; }
  string GetLocation() { return _location; }
  savime_size_t GetLength() { return _length; }
  savime_size_t GetEntryCount() { return _entry_count; };
  DataType GetType() { return _type; }
  BitsetPtr BitMask() { return _bitMask; }
  void InflateBitMask();
  savime_size_t GetBitsPerBlock();
  bool& HasIndexes() { return _has_indexes; }
  bool& Sorted() { return _sorted; }
  void Resize(savime_size_t size);
  void Redefine(int64_t id, string name, string file, DataType type);
  ~Dataset();
};
typedef std::shared_ptr<Dataset> DatasetPtr;

/**
* A dimension is the basic data element that defines the structure of a TAR. A
* dimension has a series of associated values,
* called indexes. From the users perspective, these indexes call be arbitrarily,
* irregularly spaced values called logical indexes.
* From the SAVIME internals perspective, every logical index must be mapped to
* an integer values used to specify the position of a tuple of
* values in the underlying datasets. Therefore, a dimension has a double value
* lower and upper_bounds defining its range of values (for
* implicit dimensions), or a dataset specifying explicitly the logical values
* for explicity dimension. A dimension also stores
* 64-bit integers for the real lower and upper bounds.
*/
struct Dimension : public MetadataObject {
private:
  int32_t _dimId;     /*!<Identifier of the dimension in the metadata manager.*/
  string _name;        /*!<User given dimension name.*/
  DataType _type;      /*!<Dimension data type.*/
  DimensionType _dimension_type; /*!<Dimension type (IMPLICIT or EXPLICIT).*/
  double _lower_bound;           /*!<Logical lower bound of the dimension.*/
  double _upper_bound;           /*!<Logical upper bound of the dimension.*/
  double _spacing; /*!<Spacing or distance between to adjacent/consecutive
                     logical indexes.*/

  RealIndex _real_upper_bound; /*!<Real upper bound for a dimension.*/
  RealIndex _current_upper_bound; /*!<Max upper bound filled by a subtar.*/
  DatasetPtr
      _dataset; /*!<Dataset with explicit logical indexes. It is null for
                  IMPLICIT dimensions and mandatory for EXPLICIT dimensions.*/

public:

  Dimension(int32_t id, string name, DataType type, double lower_bound,
      double upper_bound, double spacing);

  Dimension(int32_t id, string name, DatasetPtr dataset);

  int32_t GetId() { return _dimId; }
  string GetName() { return _name; }
  void AlterName(string name);
  DataType GetType() { return _type; }
  DimensionType GetDimensionType() { return _dimension_type; }
  double GetLowerBound() { return _lower_bound; }
  double GetUpperBound() { return _upper_bound; }
  double GetSpacing() { return _spacing; }
  RealIndex GetRealUpperBound() { return _real_upper_bound; }
  RealIndex& CurrentUpperBound() { return _current_upper_bound; }
  DatasetPtr GetDataset() { return _dataset; }

  /**
  * Returns the dimension length or the number of different possible index
  * values it can hold.
  * @return An unsigned 64-bit integer containing the dimension length.
  */
  savime_size_t GetLength() {
    return ((savime_size_t)((_upper_bound - _lower_bound) / _spacing)) + 1;
  }

  /**
  * Returns the current dimension length or the maximum real upper bound for any
  * subtar in the dimension.
  * @return An unsigned 64-bit integer containing the current dimension length.
  */
  savime_size_t GetCurrentLength() { return _current_upper_bound + 1; }
  
  ~Dimension();
};
typedef std::shared_ptr<Dimension> DimensionPtr;

/**
* An attribute is another basic data element that defines part of cell tuple in
* a TAR, A TAR has a set of dimensions. A set of values
* for each one of the dimensions (a.k.a. indexes) determines a position in the
* TAR. In every TAR position there can a cell or a tuple of values.
* The attributes in a TAR define which values exist in its tuples, like a column
* in the relational model.
*/
struct Attribute : public MetadataObject {
private:
  int32_t _id;    /*!<Id of the attribute in the metadata manager.*/
  string _name;   /*!<User given attribute name.*/
  DataType _type; /*!<Attribute data type (INTEGER, LONG, FLOAT, etc...).*/
public:

  Attribute(int32_t id, string name, DataType type);
  int32_t GetId() {return _id;}
  string GetName() {return _name;}
  DataType GetType() {return _type;}

  ~Attribute();
};
typedef std::shared_ptr<Attribute> AttributePtr;

/**
* A data element is a generalization of dimensions and attributes. This class
* provides a framework for coping with sets of dimensions and
* attributes as if they were a homogeneous structure. It encapsulates either a
* dimension or attribute.
*/
class DataElement : MetadataObject {
  DimensionPtr _dimension; /*!<Reference to a dimension. Null if its an
                              attribute data element.*/
  AttributePtr _attribute; /*!<Reference to an attribute. Null if its a
                              dimension data element.*/
  DataElementType _type;   /*!<Type specification (dimension or attribute).*/

public:
  /**
  * Constructor.
  * @param dimension is a dimension to be encapsulated as a data element.
  */
  DataElement(DimensionPtr dimension);

  /**
  * Constructor.
  * @param attribute is an attribute to be encapsulated as a data element.
  */
  DataElement(AttributePtr attribute);

  /**
  * Returns a dimension being encapsulated as the data element. If the instance
  * encapsulates an attribute, returns NULL.
  * @return A dimension being encapsulated or NULL.
  */
  DimensionPtr GetDimension();

  /**
  * Returns an attribute being encapsulated as the data element. If the instance
  * encapsulates a dimension, returns NULL.
  * @return An attribute being encapsulated or NULL.
  */
  AttributePtr GetAttribute();

  /**
  * Returns the type of data element being encapsulated.
  * @return DIMENSION_SCHEMA_ELEMENT if its a dimension or
  * ATTRIBUTE_SCHEMA_ELEMENT if it is an attribute .
  */
  DataElementType GetType();

  /**
  * Returns the data type of the data element regardless if it is a dimension or
  * attribute.
  * @return The data type of the data element.
  */
  DataType GetDataType();

  /**
  * Returns the user given name of the data element regardless if it is a
  * dimension or attribute.
  * @return The user given name of the data element.
  */
  std::string GetName();

  /**
  * Returns true if its a numeric typed data element and false otherwise.
  * @return True if data element type is DOUBLE, FLOAT, or an INT and false
  * otherwise.
  */
  bool IsNumeric();

  /**Destructor*/
  ~DataElement();
};
typedef std::shared_ptr<DataElement> DataElementPtr;

/**
* Every subTAR in a TAR must contain a DimensionSpecification for each TAR
*dimension. The dimension specification determines which ranges the subTAR
*comprises, and how data is ordered/structure in the datasets for that subtar.
* A dimension specification can be ORDERED, meaning the indexes values for every
*cell within the subTAR region follow a ordered well behaved pattern. If the
* specified dimension is IMPLICIT, logical dimension indexes for any position
* within the subtar can be inferred based on the dimension specification
*lower_bound, upper_bound, adjacency and skew.
* If the dimension is EXPLICIT, then ORDERED means that the real dimension
*indexes can be inferred based on the dimension specification lower_bound,
* upper_bound, adjacency adn skew, and the logical dimension indexes can
* be then obtained by using the mapping dataset in the dimension object.
*
* If the dimension specification is PARTIAL, it means that values are possibly
*sparse, but consistent with regard to other dimensions.
* Thus, the dimension specification dataset stores a list of values whose size
*equals the length of the dimension. The dimension specification
*  dataset contains either logical indexes if the dimension is IMPLICIT, or real
*indexes if the dimension is EXPLICIT. The real indexes
* are translated into logical indexes by using the dataset in the dimension.
*
* If the dimension specification is TOTAL, it means that values are sparse and
*irregular. There is no consistency with regard to other
* dimensions. Actually, if one of the dimension specifications in the subtar is
*TOTAL, then all of them also must be.
* The dimension specification dataset stores a list of values whose size equals
*the length of the entire subtar
* (one for each no null position in the subtar). The dimension specification
*dataset contains either logical indexes if the dimension
* is IMPLICIT, or real indexes if the dimension is EXPLICIT. The real indexes
*are translated into logical indexes by using the dataset
* in the dimension.
*
* The other dimension specification parameters are important to allow indexes to
*be derived. They help describe how indexes
* repeat themselves in a well defined patter. We can summarize this information
*in a handful of values, as follows:
* lower and upper bounds: Are the bounds for the dimension specification and
*range the subtar comprises in the dimension
* adjacency: Number of consecutive elements with the same index for a given
*dimension. It is also the product of all
* lengths of posterior dimensions in the subtar. The adjacency for the last
*dimension is always equals to 1.
* skew: Length of a sequence of elements indexes containing all indexes for a
*dimension. It is the product of the
* dimension length with the lengths of all posterior dimensions.
*
* Example:
*
* 2D 4x4 Matrix (i=lines and j=columns):
*
*|   |   0    |	1     |   2   |	3     |
*|   | :----: |:----: |:----: |:----: |
*|0  |	0,0   |	0,1   |	0,2   |	0,3   |
*|1  |	1,0   |	1,1   |	1,2   |	1,3   |
*|2  |	2,0   |	2,1   |	2,2   |	2,3   |
*|3  |	3,0   |	3,1   |	3,2   |	3,3   |
*
*
*|Dim	  |Order  | lw	  | up	  | adj	   |skew   |len	   | total |
*| :----: |:----: |:----: |:----: | :----: |:----: |:----: |:----: |
*| i	  |1	  |0	  | 3	  |4	   |16     | 4	   | 16    |
*| j	  |2	  |0	  | 3	  |1	   |4	   | 4	   | 16    |
*
*3D 3x3x2 Matrix
*
*|	  |	0 |   1	  |2	  |	0  |	1   |	2   |
*| :----: |:----: |:----: |:----: | :----: |:----:  |:----: |
*|0	  |0,0,0  | 0,1,0 | 0,2,0 | 0,0,1  | 0,1,1  | 0,2,1 |
*|1	  |1,0,0  | 1,1,0 | 1,2,0 | 1,0,1  | 1,1,1  | 1,2,1 |
*|2	  | 2,0,0 | 2,1,0 | 2,2,0 | 2,0,1  | 2,1,1  | 2,2,1 |
*
*
*|Dim	  |Order  | lw	  | up	  | adj	   |skew   |len	   | total |
*| :----: |:----: |:----: |:----: | :----: |:----: |:----: |:----: |
*|i	  |1	  |0	  |2	  |6	   |18     |  3    |18     |
*|j	  |2	  |0	  |2	  |2	   |6	   |  3	   |18     |
*|k	  |3	  |0	  |2	  |1	   |2	   |  3	   |18     |
*
*/
struct DimensionSpecification : MetadataObject {

private:

  int32_t _id; /*!<Id of the dimension specification in the metadata manager.*/
  DimensionPtr _dimension; /*!<Reference to the dimension associated with the
                               dimension specification.*/
  DatasetPtr
      _dataset; /*!<Reference to the dataset associated with the dimension
                  specification.*/
  DatasetPtr _materialized; /*!<Reference to a dataset containing a materialized
                              view of logical dimension indexes for the subtar.*/
  SpecsType
      _type; /*!<Type of dimension specification: ORDERED, PARTIAL or TOTAL.*/
  RealIndex
      _lower_bound; /*!<Real index of the TAR specifying the lower bound of the
                      subtar for that dimension.*/
  RealIndex
      _upper_bound;    /*!<Real index of the TAR specifying the lower of bound of
                         the subtar for that dimension.*/
  savime_size_t _skew; /*!<Length of a sequence of elements indexes containing
                       * all indexes for a dimension.
                       * It is the product of the dimension length with the
                       * lenghts of all posterior dimensions.*/
  savime_size_t _adjacency; /*!<Number of consecutive elements with the same
                            *index for a given dimension. It is also the product
                            *of all
                            *lengths of posterior dimensions in the subtar. The
                            *adjacency for the last dimension is always equals
                            *to 1.*/

public:

  DimensionSpecification(int32_t id, DimensionPtr dimension, RealIndex lowerBound,
      RealIndex upperBound, savime_size_t skew, savime_size_t adjacency);

  DimensionSpecification(int32_t id, DimensionPtr dimension, DatasetPtr dataset, RealIndex lowerBound,
                         RealIndex upperBound, savime_size_t skew, savime_size_t adjacency);

  DimensionSpecification(int32_t id, DimensionPtr dimension, DatasetPtr dataset, RealIndex lowerBound,
                         RealIndex upperBound);

  int32_t GetId() { return _id;}
  DimensionPtr GetDimension() { return _dimension;}
  DatasetPtr GetDataset() { return _dataset;}
  DatasetPtr& GetMaterialized() { return _materialized;}
  SpecsType GetSpecsType() { return _type;}
  RealIndex GetLowerBound() { return _lower_bound;}
  RealIndex GetUpperBound() { return _upper_bound;}
  savime_size_t GetSkew() { return _skew;}
  savime_size_t GetAdjacency() { return _adjacency;}

  void AlterDimension(DimensionPtr dimension);
  void AlterSkew(savime_size_t skew);
  void AlterAdjacency(savime_size_t adj);
  void AlterBoundaries(RealIndex lowerBound, RealIndex upperBound);
  void AlterDataset(DatasetPtr dataset);

  /**
  * Returns the filled length of the dimension specification.
  * @return An 64-bit integer containing the number of indexes in
  * a dimension for which the subtar actually holds data.
  */
  savime_size_t GetFilledLength() {
    savime_size_t filledLength;

    if (_type == ORDERED) {
      filledLength = _upper_bound - _lower_bound + 1;
    } else if (_type == PARTIAL || _type == TOTAL) {
      filledLength = _dataset->GetEntryCount();
    }

    return filledLength;
  }

  /**
  * Returns the spanned length of the dimension specification.
  * @return An 64-bit integer containing the number of all possible indexes
  * for the dimension within the TAR region covered by the SubTAR.
  */
  savime_size_t GetSpannedLength() {
    savime_size_t spannedLength;
    spannedLength = _upper_bound - _lower_bound + 1;
    return spannedLength;
  }

  std::shared_ptr<DimensionSpecification> Clone();
};
typedef std::shared_ptr<DimensionSpecification> DimSpecPtr;

bool compareAdj(DimSpecPtr a, DimSpecPtr b);

/**
* A TAR region is instantiated with data as a subTAR. A subTAR encompasses
* an n-dimensional slice of a TAR. Every subTAR is defined by the TAR region
* it represents.
*SubTARs not only define a partitioning scheme for a TAR, but also serve
*as a way to allow users to specify the details about how their data is laid
*out, avoiding costly data transformations and rearrangements during ingestion into
*the Savime.
*/
class Subtar : MetadataObject {

private:
  int32_t _id; /*!<Id of the subtar in the metada manager.*/
  int32_t
      _id_tar; /*!<Id of the tar to which the subtar belogs to in the metada
                  manager.*/
  TARPtr _tar; /*!<Reference to tar to which the subtar belongs to.*/
  map<string, DimSpecPtr> _dimSpecs; /*!<Maps between dimension names and
                                        dimension specifications references.*/
  map<string, DatasetPtr>
      _dataSets; /*!<Maps between attribute names and datasets holding data.*/

public:
  /**
  * Constructor.
  */
  Subtar(){};

  /**
  * Returns the subtar Id.
  * @return A 32-bit integer containing the subtar id.
  */
  int32_t GetId();

  /**
  * Sets the subtar id.
  * @param id is a 32-bit integer containing the id to be set.
  */
  void SetId(int32_t id);

  /**
  * Sets the subtar associated TAR.
  * @param tar is the TAR to which the subtar is associated to.
  */
  void SetTAR(TARPtr tar);

  /**
  * Returns the TAR the subtar is associated to.
  * @return A TAR reference to which the subtar is associated to.
  */
  TARPtr GetTAR();

  /**
  * Returns the dimension specifications for the subtar.
  * @return A map reference mapping dimension names to their respective
  * dimension specifications in the subtar.
  */
  std::map<string, DimSpecPtr> &GetDimSpecs();

  /**
  * Returns the datasets associated to the subtar.
  * @return A map reference mapping attribute names to their respectives
  * datasets.
  */
  std::map<string, DatasetPtr> &GetDataSets();

  /**
  * Returns total length of cells actually present in a subtar.
  * @return A 64-bit containing the total number of cells/tuples in the subtar.
  * datasets.
  */
  savime_size_t GetFilledLength();

  /**
  * Returns total length of a subtar.
  * @return A 64-bit containing the total number of cells/tuples in the TAR
  * region covered by the subtar.
  */
  savime_size_t GetSpannedLength();

  /**
  * Adds a new dimension specification to the subtar.
  * @param dimension is the dimension the dimension specification is associated
  * to. It
  * must be present in the the associated TAR.
  * @param  offset is reserved for future use, default to zero.
  * @param  lowerBound is the dimension specification lower bound.
  * @param  upperBound is the dimension specification upper bound.
  * @param  adjacency is the dimension specification adjacency.
  * @param  skew is the dimension specification skew.
  * @param  type is the dimension specification type.
  * @param  associatedDataSet is dimension specification dataset.
  */
  void AddDimensionsSpecification(DataElementPtr dimension,
                                  savime_size_t offset, RealIndex lowerBound,
                                  RealIndex upperBound, savime_size_t adjacency,
                                  savime_size_t skew, SpecsType type,
                                  DatasetPtr associatedDataSet);

  /**
  * Adds an already created new dimension specification to the subtar.
  * @param dimSpecs is the dimension specification to be added in the subtar.
  */
  void AddDimensionsSpecification(DimSpecPtr dimSpecs);

  /**
  * Attaches a new dataset to an attribute.
  * @param dataElementName is the name of a valid TAR attribute.
  * @param dataset is the dataset to be attached.
  */
  void AddDataSet(string dataElementName, DatasetPtr dataset);

  /**
  * Fills the arrays with the minimum and maximum real indexes the subtar
  * comprises
  * in each dimension. Arrays must be preallocated.
  * @param min is a n sized array for storing lower bounds.
  * @param max is a n sized array for storing upper bounds.
  * @param n is the number of dimensions to be considered. If n is smaller than
  * the actual number of dimensions, only the n first dimensions in the subtar
  * will be considered.
  */
  void CreateBoundingBox(int64_t min[], int64_t max[], int32_t n);

  /**
  * Returns the dimension specification reference for a dimension.
  * @name name is the name of the dimension for which the dimension
  * specification
  * must be retrieved.
  * @return The solicited dimension specification reference or NULL if it does
  * not exist.
  */
  DimSpecPtr GetDimensionSpecificationFor(string name);

  /**
  * Returns the dimension specification reference for a dimension.
  * @name name is the name of the dimension for which the dimension
  * specification
  * must be retrieved.
  * @return The solicited dimension specification reference or NULL if it does
  * not exist.
  */
  DatasetPtr GetDataSetFor(string name);

  /**
  * Removes references to temporary dimension specifications and datasets added
  * during query execution.
  */
  void RemoveTempDataElements();

  /**
  * Checks where the argument subtar intersects with this subtar. Two subtars
  * intersect if
  * all their dimensions are the same and there is an intersection between their
  * lower and upper
  * bounds.
  * @name subtar is the subtar reference to check for intersection.
  * @return True if the subtar intersects and false otherwise.
  */
  bool IntersectsWith(SubtarPtr subtar);

  /**
  * Returns a string representation of the subtar.
  * @return A string representing the subtar object.
  */
  string toString();

  /**
   * Destructor
   */
  ~Subtar();
};

/**
 * A role is semantically meaningful data element associated to a type.
 */
struct Role : MetadataObject {
  int32_t id;        /*!<Id of the role in the metada manager.*/
  string name;       /*!<User given role name.*/
  bool is_mandatory; /*!<Mandatory roles are the ones that must be implemented
                      * so the TAR can be said to have the given type.*/
};
typedef std::shared_ptr<Role> RolePtr;

/**
 * A type defines the general semantics of data elements in a TAR.
 * It comprises a set of roles, that must be implemented or fulfilled by
 * the data elements in the TAR so it can be considered to be of
 * the given type.
 */
struct Type : MetadataObject {
  int32_t id;                 /*!<Id of the type in the metada manager.*/
  string name;                /*!<User given type name.*/
  map<string, RolePtr> roles; /*!<Maps between role names and role references
                               * associated to the type.*/
};
typedef std::shared_ptr<Type> TypePtr;

/**
* A TAR (Typed Array) is the basic storage structure for Savime's TARS data
* model.
* A TAR has a set of data elements: dimensions and attributes. A
* TAR cell is a tuple of attributes accessed by a set of indexes. These indexes
* define
* the cell location within the TAR. A TAR has a type, formed by a set of roles.
* A
* role in a type defines a special purpose data element with specific semantics.
* If a
* TAR is of a given type T, it is guaranteed to have a set of data elements that
* fulfill the roles defined in T . This facilitates the creation of operations
* that require
* additional semantics about the data.
*/
class TAR : MetadataObject {

private:
  int32_t _id;                /*!<Id of the TAR in the metada manager.*/
  int32_t _idType;            /*!<Id of the TAR type in the metada manager.*/
  TypePtr _type;              /*!<Reference to the TAR type.*/
  SubtarsIndex _subtarsIndex; /*!<RTree index containing subtars.*/
  string _name;               /*!<User given TAR name.*/
  list<DataElementPtr>
      _elements; /*!<List of TAR data elements forming its schema.*/
  vector<SubtarPtr> _subtars; /*!<Vector of subTARs.*/
  vector<int32_t> _idSubtars; /*!<Vector of subTARs ids in the metada manager.*/
  map<string, RolePtr>
      _roles; /*!<Map between data element names and their roles.*/

  /*Static members*/
  static mutex
      _mutex; /*!<Mutex for controlling access to intersecting subtars list.*/
  static vector<int64_t>
      _intersectingSubtarsIndexes; /*!<Auxiliary list for retrieving
                                      intersecting subtars.*/

  /**
  * Auxiliary member function that validates a TAR name string.
  * @param name is a string to be validated.
  * @return True if it's a valid TAR name and false otherwise.
  */
  bool validateTARName(string name);

  /**
  * Auxiliary member function that validates a data element name string.
  * @param name is a string to be validated.
  * @return True if it's a valid data element name and false otherwise.
  */
  bool validateSchemaElementName(string name);

  /**
  * Auxiliary member function that verifies if a subtar is compliant with
  * the requirements to be added to the TAR's subtar list.
  * @param subtar is a subtar reference to be validated.
  */
  void validadeSubtar(SubtarPtr subtar);

  /**
  * Callback function called by the SubtarsIndex structure when
  * retrieving subtars that intersect with one another.
  * @param id is the position of the next found intersecting subtar
  * in the vector _subtars.
  * @return Always True, so the SubtarsIndex keeps the search.
  */
  static bool _callback(int64_t id, void *arg) {
    _intersectingSubtarsIndexes.push_back(id);
    return true;
  }

public:
  /**
  * Default constructor.
  */
  TAR(){};

  /**
  * Constructor.
  * @param id is the TAR id in the metada manager.
  * @param id is the TAR user given name.
  * @param type is the reference to the TAR type.
  */
  TAR(int32_t id, string name, TypePtr type);

  
  /**
  * Adds a new attribute data element to the TAR schema depending on all
  * dimensions.
  * @param name is the new attribute name.
  * @param type is the new attribute data type.
  * @return True if the attribute was added and false otherwise.
  */
  void AddAttribute(string name, DataType type);

  /**
  * Adds a new already created attribute reference to the TAR schema.
  * @param attribute is the new attribute to be added.
  */
  void AddAttribute(AttributePtr attribute);

  /**
  * Adds a new implicit dimension data element to the TAR schema with spacing
  * equals to 1.
  * @param name is the new dimension name.
  * @param type is the new dimension data type.
  * @param lowerBound is smallest possible logical index for that dimension.
  * @param upperBound is largest possible logical index for that dimension.
  * @param current_upper_bound is current largest logical filled.
  */
  void AddDimension(string name, DataType type, double lowerBound,
                    double upperBound, int64_t current_upper_bound);

  /**
  * Adds a new implicit dimension data element to the TAR schema defining its
  * spacing.
  * @param name is the new dimension name.
  * @param type is the new dimension data type.
  * @param lowerBound is smallest possible logical index for that dimension.
  * @param upperBound is largest possible logical index for that dimension.
  * @param realLower is smallest possible real index for that dimension.
  * @param realUpper is largest possible real index for that dimension.
  * @param spacing is the difference between two adjacent logical indexes.
  * @param current_upper_bound is current largest logical filled.
  */
  void AddDimension(string name, DataType type, double lowerBound,
                    double upperBound, RealIndex realLower, RealIndex reaUpper,
                    double spacing, int64_t current_upper_bound);

  /**
  * Adds a new explicit dimension data element to the TAR schema.
  * @param name is the new dimension name.
  * @param type is the new dimension data type.
  * @param lowerBound is smallest possible logical index for that dimension.
  * @param upperBound is largest possible logical index for that dimension.
  * @param realLower is smallest possible real index for that dimension.
  * @param realUpper is largest possible real index for that dimension.
  * @param dataset is the dataset containing the mapping between real and
  * logical indexes.
  * @param current_upper_bound is current largest logical filled.
  */
  void AddDimension(string name, DataType type, double lowerBound,
                    double upperBound, RealIndex realLower, RealIndex reaUpper,
                    int64_t current_upper_bound, DatasetPtr dataset);

  /**
  * Adds a new already created dimension data element to the TAR schema.
  * @param dimension is the dimension reference to be added.
  */
  void AddDimension(DimensionPtr dimension);

  /**
  * Set the role of a data element in the TAR.
  * @param dataElementName is the name of the data element to set the role to.
  * @param role present in the TAR type reference.
  */
  void SetRole(string dataElementName, RolePtr role);

  /**
  * Adds a new subtar to the subtars vector and to the subtars index.
  * @param subtar to be added in the TAR.
  */
  void AddSubtar(SubtarPtr subtar);

  /**
  * Gets the TAR id.
  * @return A 32-bit integer containing the TAR id.
  */
  const int32_t GetId();

  /**
  * Gets the TAR name.
  * @return A string containing the TAR name.
  */
  const string GetName();

  /**
  * Gets the TAR type.
  * @return A Type reference to the TAR type.
  */
  TypePtr GetType();

  /**
  * Gets the TAR type id.
  * @return A 32-bit integer containing the type id.
  */
  int32_t GetTypeId();

  /**
  * Sets the TAR type id.
  * @param idType is a 32-bit integer containing the type id.
  */
  void SetTypeId(int32_t idType);

  /**
  * Modifies some basic TAR info.
  * @param newId is a 32-bit integer containing the new TAR id.
  * @param newName is a string containing the new TAR name.
  */
  void AlterTAR(int32_t newId, string newName);

  /**
  * Modifies the TAR type and clears the roles map.
  * @param type is a Type reference for the new TAR type.
  */
  void AlterType(TypePtr type);

  /**
  * Gets TAR dimensions.
  * @return A list of Dimension references for the dimensions in the TAR.
  */
  list<DimensionPtr> GetDimensions();

  /**
  * Gets TAR attributes.
  * @return A list of Attribute references for the attributes in the TAR.
  */
  list<AttributePtr> GetAttributes();

  /**
  * Gets TAR roles implementations.
  * @return A map mapping between data element names and the roles they
  * implement/fulfill.
  */
  map<string, RolePtr> &GetRoles();

  /**
  * Gets the TAR subtars vector.
  * @return A reference to vector containing all subtars in the TAR.
  */
  vector<SubtarPtr> &GetSubtars();

  /**
  * Gets the TAR subtars id vector.
  * @return A reference to vector containing all ids of subtars in the TAR.
  */
  vector<int32_t> &GetIdSubtars();

  /**
  * Gets the all subtars in the TAR that intersects with the parameter subtar.
  * @param subtar is a Subtar reference to a subtar whose intersections must be
  * checked.
  * @return A vector containing all Subtar references to all intersecting
  * subtars in the TAR.
  */
  vector<SubtarPtr> GetIntersectingSubtars(SubtarPtr subtar);

  /**
  * Checks if the TAR has a data element with the given name.
  * @return True if the TAR has a data element with the given name and false
  * otherwise.
  */
  bool HasDataElement(string name);

  /**
  * Gets a data element with a given name from the TAR schema.
  * @param name is a string with the name of the data element to be retrieved.
  * @return A reference to a DataElement or NULL if it does not exist.
  */
  DataElementPtr GetDataElement(string name);

  /**
  * Gets a list of all data elements in the TAR schema.
  * @return A list with DataElement references to all data elements in the TAR.
  */
  list<DataElementPtr> &GetDataElements();

  /**
  * Removes a data element from the TAR schema with the given name.
  * @param name string with the name of the data element to be removed.
  */
  void RemoveDataElement(string name);

  /**
  * Creates a compact string representation of the TAR.
  * @return A string containing a compact textual representation of the TAR.
  */
  string toSmallString();

  /**
  * Creates a string representation of the TAR.
  * @return A string containing a textual representation of the TAR.
  */
  string toString();

  /**
  * Removes temporary data elements created during query processing.
  */
  void RemoveTempDataElements();

  /**
   * Redefines TAR dimensions according to present subtars.
   */
  void RecalculatesRealBoundaries();

  /**
   * Checks whether the TAR definition spans a region larger than max allowed.
   * @return true if TAR spans a region too large and false otherwise.
   */
  bool CheckMaxSpan();

  /**
   * Calculates the spanned length of a TAR, or the product of the current
   * real upper bound of all dimensions.
   * @return an unsigned 64-bit integer with the spanned length of the TAR.
   */
  savime_size_t GetSpannedTARLen();

  /**
  * Calculates the filled length of a TAR, or the sum of the
  * filled lengths of all subtars.
  * @return an unsigned 64-bit integer with the filled length of the TAR.
  */
  savime_size_t GetFilledTARLen();

  /**
  * Calculates the total length of a TAR, or the product of the
  * lengths of all dimensions.
  * @return an unsigned 64-bit integer with the filled length of the TAR.
  */
  savime_size_t GetTotalTARLen();

  /**
  * Clones the TAR reference according to the specified parameters.
  * @param copyId is a flag indicating whether the id should be copied to the
  * new TAR.
  * @param copySubtars is a flag indicating whether the subtars should be copied
  * to the new TAR.
  * @param dimensionsOnly is a flag indicating whether only the dimensions
  * should be copied to the new TAR.
  * @return A TAR reference clone from the original TAR.
  */
  TARPtr Clone(bool copyId, bool copySubtars, bool dimensionsOnly);

  /**
   * Destructor
   */
  ~TAR();
};

/**
 * TARS (Typed ARray Schema) is the structure that holds a series of TARs, Types
 * and Datasets.
 * It similar to a schema/database in a DBMS, and its functions is only to group
 * all metadata
 * objects under a common base object.
 */
struct TARS : MetadataObject {
  int32_t id;        /*!<Id of the TARS in the metada manager.*/
  string name;       /*!<User given TARS name.*/
  list<TARPtr> tars; /*!<List containing references to all TARs in the TARS.*/
  list<int32_t> id_tars; /*!<List containing ids for all TARs in the TARS.*/
  list<TypePtr>
      types; /*!<List containing references to all Types in the TARS.*/
  list<int32_t> id_types; /*!<List containing ids for all Types in the TARS.*/
  list<DatasetPtr>
      datasets; /*!<List containing references to all Datasets in the TARS.*/
  list<int32_t>
      id_datasets; /*!<List containing ids for all Datasets in the TARS.*/

  /**
   * Destructor.
   */
  ~TARS();
};
typedef std::shared_ptr<TARS> TARSPtr;

/**
* The metadata manager is the module responsible for saving, persisting and
* retrieving all Metadata Objects in the system.
*/
class MetadataManager : public SavimeModule {

public:
  /**
  * Constructor.
  * @param configurationManager is a reference to the standard system
  * ConfigurationManager.
  * @param systemLogger is a reference to the standard SystemLogger.
  */
  MetadataManager(ConfigurationManagerPtr configurationManager,
                  SystemLoggerPtr systemLogger)
      : SavimeModule("Metada Manager", configurationManager, systemLogger) {}

  /**
  * Saves a new TARS in the metadata manager underlying storage.
  * @param tars is the TARS reference to be saved.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult SaveTARS(TARSPtr tars) = 0;

  /**
  * Retrieves a TARS reference from the underlying metadata manager storage.
  * @param id is 32-bit integer id of the TARS to be retrieved.
  * @return A TARS reference if found and NULL otherwise.
  */
  virtual TARSPtr GetTARS(int32_t id) = 0;

  /**
  * Deletes the TARS from the metadata manager underlying storage.
  * @param tars is the TARS reference to be deleted.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult RemoveTARS(TARSPtr tars) = 0;

  /**
  * Saves a new TAR in the metadata manager underlying storage.
  * @param tars is the TARS reference in which the TAR is to be saved.
  * @param tar is the TAR reference to be saved.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult SaveTAR(TARSPtr tars, TARPtr tar) = 0;

  /**
  * Retrieves a TAR reference from the underlying metadata manager storage.
  * @param  tars is the TARS in which the TAR should be looked for.
  * @param  tarName is string with the TAR name to be retrieved.
  * @return A TAR reference if found and NULL otherwise.
  */
  virtual TARPtr GetTARByName(TARSPtr tars, string tarName) = 0;

  /**
  * Retrieves all TARs references for a TARS from the underlying metadata
  * manager storage.
  * @param  tars is the TARS for which the TARs should be retrieved.
  * @return A list of TAR references contained in the TARS.
  */
  virtual list<TARPtr> GetTARs(TARSPtr tars) = 0;

  /**
  * Deletes the TAR from the metadata manager underlying storage.
  * @param tars is the TARS reference from which the TAR should be deleted.
  * @param tar is the TAR reference that should be deleted.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult RemoveTar(TARSPtr tars, TARPtr tar) = 0;

  /**
  * Saves a new subTAR in the metadata manager underlying storage.
  * @param tar is the TAR reference in which the subTAR is to be saved.
  * @param subtar is the subTAR reference to be saved.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult SaveSubtar(TARPtr tar, SubtarPtr subtar) = 0;

  /**
  * Retrieves all subTARs references for a TAR from the underlying metadata
  * manager storage.
  * @param  tarName is the name of the TAR for which the subTARs should be
  * retrieved.
  * @return A list of Subtar references contained in the TAR.
  */
  virtual list<SubtarPtr> GetSubtars(std::string tarName) = 0;

  /**
  * Retrieves all subTARs references for a TAR from the underlying metadata
  * manager storage.
  * @param  tar is the reference of the TAR for which the subTARs should be
  * retrieved.
  * @return A list of Subtar references contained in the TAR.
  */
  virtual list<SubtarPtr> GetSubtars(TARPtr tar) = 0;

  /**
  * Deletes the subTAR from the metadata manager underlying storage.
  * @param tar is the TAR reference from which the subTAR should be deleted.
  * @param subtar is the Subtar reference that should be deleted.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult RemoveSubtar(TARPtr tar, SubtarPtr subtar) = 0;

  /**
  * Saves a new Type in the metadata manager underlying storage.
  * @param tars is the TARS reference in which the Type is to be saved.
  * @param type is the Type reference to be saved.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult SaveType(TARSPtr tars, TypePtr type) = 0;

  /**
  * Retrieves a Type reference from the underlying metadata manager storage.
  * @param  tars is the TARS in which the Type should be looked for.
  * @param  typeName is string with the Type name to be retrieved.
  * @return A Type reference if found and NULL otherwise.
  */
  virtual TypePtr GetTypeByName(TARSPtr tars, string typeName) = 0;

  /**
  * Retrieves all Type references for a TARS from the underlying metadata
  * manager storage.
  * @param  tars is the TARS for which the Types should be retrieved.
  * @return A list of Type references contained in the TARS.
  */
  virtual list<TypePtr> GetTypes(TARSPtr tars) = 0;

  /**
  * Deletes the Type from the metadata manager underlying storage.
  * @param tars is the TARS reference from which the Type should be deleted.
  * @param tar is the Type reference that should be deleted.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult RemoveType(TARSPtr tars, TypePtr type) = 0;

  /**
  * Saves a new Dataset in the metadata manager underlying storage.
  * @param tars is the TARS reference in which the Dataset is to be saved.
  * @param dataset is the Dataset reference to be saved.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult SaveDataSet(TARSPtr tars, DatasetPtr dataset) = 0;

  /**
  * Retrieves a Dataset reference from the underlying metadata manager storage.
  * @param  tars is the TARS in which the Dataset should be looked for.
  * @param  dsName is string with the Dataset name to be retrieved.
  * @return A Dataset reference if found and NULL otherwise.
  */
  virtual DatasetPtr GetDataSetByName(string dsName) = 0;

  /**
  * Deletes the Dataset from the metadata manager underlying storage.
  * @param tars is the TARS reference from which the Dataset should be deleted.
  * @param tar is the Dataset reference that should be deleted.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult RemoveDataSet(TARSPtr tars, DatasetPtr dataset) = 0;

  /**
  * Checks if a metadata object is valid and not already in use.
  * @param identifier is a string containing the identifier to be validated.
  * @param objectType is a string specifying the type of metadata object.
  * @return True if its valid and false otherwise.
  */
  virtual bool ValidateIdentifier(string identifier, string objectType) = 0;
  
  /**
  * Register a query in the logical schema backup.
  * @param query is a string containing a DML query to be register.
  */
  virtual void RegisterQuery(string query) = 0;
  
  /**
  * Get a list of queries in the logical backup.
  * @return the current list of queries in the logical backup.
  */
  virtual list<string> GetQueries() = 0;
};
typedef std::shared_ptr<MetadataManager> MetadataManagerPtr;

/**
* Get a DimensionType code according to a C string representation.
* @param type is the C string to be parsed to a DimensionType.
* @return A DimensionType obtained after parsing the C string.
*/
DimensionType STR2DIMTYPE(const char *type);

/**
* Get a SpecsType code according to a C string representation.
* @param type is the C string to be parsed to a SpecsType.
* @return A SpecsType obtained after parsing the C string.
*/
SpecsType STR2SPECTYPE(const char *type);

#endif

