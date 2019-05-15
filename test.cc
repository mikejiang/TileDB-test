/*
 * test.cc
 *
 *  Created on: May 13, 2018
 *      Author: wjiang2
 */

#include "tiledb_sparse_header.h"
#include <tiledb/tiledb>

using namespace std;
void create_tiledb(const string & dbdir, const string & attr, array<unsigned, 2> row_domain, array<unsigned, 2> col_domain) {
  // Create TileDB context
  tiledb::Context ctx;

  // Create dimensions
    auto d1 = tiledb::Dimension::create<unsigned>(ctx, "row", row_domain,3);
    auto d2 = tiledb::Dimension::create<unsigned>(ctx, "col", col_domain,2);

  //  auto d1 = tiledb::Dimension::create<uint64_t>(ctx, "row", {{1, 4}}, 2);
//  auto d2 = tiledb::Dimension::create<uint64_t>(ctx, "col", {{1, 4}}, 2);

  // Create domain
  tiledb::Domain domain(ctx);
  domain.add_dimension(d1).add_dimension(d2);

  // Create attributes
  tiledb::Attribute a1 = tiledb::Attribute::create<int>(ctx, attr);
  tiledb::Filter filter1(ctx, TILEDB_FILTER_LZ4);
  filter1.set_option(TILEDB_COMPRESSION_LEVEL, -1);
  tiledb::FilterList filter_list1(ctx);
  filter_list1.add_filter(filter1);
  a1.set_filter_list(filter_list1);

  // Create array schema
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}});
//  schema.set_capacity(2);
  schema.set_domain(domain);
  schema.add_attributes(a1);

  // Check array schema
//  try {
    schema.check();
    schema.dump();
//  } catch (tiledb::TileDBError& e) {
//    std::cout << e.what() << "\n";
//    return -1;
//  }

  // Create array
  tiledb::Array::create(dbdir, schema);

  // Nothing to clean up - all C++ objects are deleted when exiting scope

//  return 0;
}

//void write_tiledb(const string & dbdir, const string & attr, vector<int> & data) {
//  // Create TileDB context
//  tiledb::Context ctx;
//  tiledb::Array array(ctx, dbdir, TILEDB_WRITE);
//
//  // Create query
//  tiledb::Query query(ctx, array, TILEDB_WRITE);
//  query.set_layout(TILEDB_GLOBAL_ORDER);
////  query.set_subarray<unsigned>({ridx[0], ridx[1], cidx[0], cidx[1]});
//
//  query.set_buffer(attr, data);
////  query.set_buffer("a2", a2_buff);
////  query.set_buffer("a3", a3_buff);
////  query.set_coordinates(coords);
//
//  // Submit query
//  query.submit();
//
////  tiledb_query_reset_buffers();
////  query.set_buffer(attr, data.data()+8, 8);
//  // Finalize query
//  query.finalize();
//
//  // Nothing to clean up - all C++ objects are deleted when exiting scope
//
////  return 0;
//}
void write_tiledb_sparse(const string & dbdir, const string & attr, vector<int> & data) {
  // Create TileDB context
  tiledb::Context ctx;

  // Create query
  tiledb::Array array(ctx, dbdir, TILEDB_WRITE);

   // Create query
   tiledb::Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
//  query.set_subarray<unsigned>({ridx[0], ridx[1], cidx[0], cidx[1]});

  query.set_buffer(attr, data.data(), 3);
//  query.set_buffer("a2", a2_buff);
//  query.set_buffer("a3", a3_buff);
  std::vector<unsigned> coords = {1,1,2,1,3,3};
  query.set_coordinates(coords);

  // Submit query
  query.submit();

//  tiledb_query_reset_buffers();
//  query.set_buffer(attr, data.data()+2, 2);
//  std::vector<unsigned> coords1 = {1,2,3,3};
//  query.set_coordinates(coords1);
//  query.submit();

  // Finalize query
//  query.finalize();
  array.close();
  // Nothing to clean up - all C++ objects are deleted when exiting scope

//  return 0;
}

void query_dim(const string & dbdir){
	tiledb::Context ctx;
	  tiledb::ArraySchema as(ctx, dbdir);

	  auto dm = as.domain().dimensions();
	  for(auto d : dm)
	  {
		  std::cout << d.name() << std::endl;
		  auto v = d.domain<unsigned>();
		  std::cout << v.first << ": " << v.second << std::endl;
	  }

}
void point_selection_tiledb(const string & dbdir, const string & attr, int * buf, vector<unsigned> ridx, vector<unsigned> cidx) {
  // Create TileDB Context
	tiledb::Context ctx;
	 // Create query
	tiledb::Array array(ctx, dbdir, TILEDB_READ);

	// Create query
	tiledb::Query query(ctx, array, TILEDB_READ);
	  tiledb::Subarray subarray(ctx, array, TILEDB_UNORDERED);

//  dims[0].domain()
//check ridx,cidx and buf size
  int cnt = 0;
  for(auto j : cidx)
  {
	  unsigned range[] = {j,j};
	  subarray.add_range(1, range);

	  for(auto i : ridx)
	  {
		  unsigned range[] = {i, i};

		  subarray.add_range(0, range);

//		  auto result_el = query.result_buffer_elements();
//		  if(result_el["a1"].second > 0)
//			  cout << buf[cnt-1] << endl;
//		  cnt++;
	  }
  }
  query.set_layout(TILEDB_COL_MAJOR);
  query.set_subarray(subarray);

  query.set_buffer(attr, &(buf[cnt++]), 1);
  query.submit();
  query.finalize();

//  for (unsigned i = 0; i < result_el["a1"].second; ++i)
//    std::cout << a1_data[i] << "\n";

  // Nothing to clean up - all C++ objects are deleted when exiting scope

//  return 0;
}
void region_selection_tiledb(const string & dbdir,  const string & attr, int * buf, uint64_t size, vector<unsigned> ridx, vector<unsigned> cidx) {
  // Create TileDB Context
  tiledb::Context ctx;

//check ridx,cidx and buf size
	  // Create query
  tiledb::Array array(ctx, dbdir, TILEDB_READ);

  	  tiledb::Query query(ctx, array, TILEDB_READ);
  	  query.set_layout(TILEDB_COL_MAJOR);
  	  const std::vector<unsigned> subarray = {ridx[0], ridx[1], cidx[0], cidx[1]};
  	  query.set_subarray(subarray);
//	  auto max_sizes = tiledb::Array::max_buffer_elements(ctx, dbdir, subarray);
//	  std::vector<unsigned> coords_buff(max_sizes[TILEDB_COORDS].second);

	  query.set_buffer(attr, buf, size);
//	  query.set_coordinates(coords_buff);

	  query.submit();
//	  query.finalize();
	  array.close();

//	  auto result_el = query.result_buffer_elements();
//	  auto nElements = result_el[attr].second;
//	  auto nrow = ridx[1] - ridx[0] + 1;
//	  //repopulate buf by assigning elements to its right position
//	  for(int i = nElements - 1; i >=0; i--)
//	  {
//		  auto coord_idx = 2*i;
//		  auto r_offset = coords_buff[coord_idx] - ridx[0];
//		  auto c_offset = coords_buff[coord_idx + 1] - cidx[0];//convert to zero-based idx
//		  auto offset = c_offset * nrow + r_offset;
//		  swap(buf[offset], buf[i]);
//	  }
//	  auto result_el = query.result_buffer_elements();
//	  if(result_el["a1"].second > 0)
//		  cout << buf[cnt-1] << endl;
//		  cnt++;


//  for (unsigned i = 0; i < result_el["a1"].second; ++i)
//    std::cout << a1_data[i] << "\n";

  // Nothing to clean up - all C++ objects are deleted when exiting scope

//  return 0;
}
int main(){
	string dbfile = "my_sparse_array";
	string attr = "a1";

//	array<unsigned, 2> row_domain={{1,4}};
//	array<unsigned, 2> col_domain={{1,4}};
//	create_tiledb(dbfile, attr, row_domain, col_domain);
	// Prepare cell buffers
	std::vector<int> data = {0, 1, 2, 3, 4, 5, 6, 7,8,9,10,11,12,13,14,15};
//	std::vector<int> data = {0, 1, 2, 3, 4, 5, 6, 7,8,9,10,11,12,13,14,15};
//	std::vector<unsigned> coords = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1, 4, 2, 3, 3, 3, 4};
	query_dim(dbfile);
	std::vector<int> data1 = {1,2,3};
	write_tiledb_sparse(dbfile, attr,data1);
//	write_tiledb(dbfile, attr, data);
//	write_tiledb(dbfile, attr, data, {1,4}, {3,4});
	int output[6]={0};
	vector<unsigned> i = {1,2,3};
	vector<unsigned> j = {1,1,3};
	point_selection_tiledb(dbfile, attr, output, i, j);
	for(auto v : output)
		cout << v << ", ";
	cout << endl;

	vector<unsigned> ridx = {1,4};
	vector<unsigned> cidx = {1,4};
	int output1[16]={0};
	region_selection_tiledb(dbfile, attr, output1, 16, ridx, cidx);
	for(auto v : output1)
			cout << v << ", ";
		cout << endl;
}

